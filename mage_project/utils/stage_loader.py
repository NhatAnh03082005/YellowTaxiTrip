import json
import os
import re
from io import StringIO
import uuid
from urllib.parse import quote_plus

import pandas as pd
from sqlalchemy import create_engine, inspect, text
from .profiling import phase
from .canonical import _json_value
import numpy as np


IDENTIFIER = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')
INTEGER_TYPES = re.compile(r'\b(smallint|integer|bigint)\b')


class _CopyBufferChain:
    """Expose many bounded CSV buffers as one lazy file-like COPY stream."""

    def __init__(self, buffers):
        self.buffers = iter(buffers)
        self.current = None

    def _next(self):
        if self.current is not None:
            self.current.close()
        self.current = next(self.buffers, None)
        return self.current is not None

    def read(self, size=-1):
        if size is None or size < 0:
            pieces = []
            while self.current is not None or self._next():
                value = self.current.read()
                if value:
                    pieces.append(value)
                self.current.close()
                self.current = None
            return b''.join(pieces)
        pieces = []
        remaining = size
        while remaining:
            if self.current is None and not self._next():
                break
            value = self.current.read(remaining)
            if value:
                pieces.append(value)
                remaining -= len(value)
            else:
                self.current.close()
                self.current = None
        return b''.join(pieces)

    def close(self):
        if self.current is not None:
            self.current.close()
            self.current = None
        for buffer in self.buffers:
            buffer.close()


def postgres_engine():
    password = quote_plus(os.environ['POSTGRES_PASSWORD'])
    url = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format(
        os.environ['POSTGRES_USER'], password, os.environ['POSTGRES_HOST'],
        os.environ.get('POSTGRES_PORT', '5432'), os.environ['POSTGRES_DB'],
    )
    return create_engine(url)


def _safe_identifier(value):
    if not IDENTIFIER.match(value):
        raise ValueError('Unsafe SQL identifier: {}'.format(value))
    return value


def serialize_json_value(value):
    """Keep missing values as SQL NULL; serialize real JSON values only."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if not isinstance(value, (dict, list)):
        try:
            if bool(pd.isna(value)):
                return None
        except (TypeError, ValueError):
            pass
    return json.dumps(value, ensure_ascii=False, default=str)


def normalize_copy_frame(dataframe, target_type_names):
    """Format nullable integer columns as `1`, never pandas' float `1.0`."""
    result = dataframe.copy(deep=False)
    for column, type_name in target_type_names.items():
        if column not in result.columns or not INTEGER_TYPES.search(type_name.lower()):
            continue
        original = result[column]
        numeric = pd.to_numeric(original, errors='coerce')
        invalid = original.notna() & numeric.isna()
        fractional = numeric.notna() & ((numeric % 1) != 0)
        if invalid.any() or fractional.any():
            bad_values = original.loc[invalid | fractional].head(5).tolist()
            raise ValueError(
                'Column {} contains non-integer values for {}: {}'.format(
                    column, type_name, bad_values
                )
            )
        result[column] = numeric.astype('Int64')
    return result


def prepare_raw_frame(dataframe, target_type_names):
    """Typed Raw is a transport projection; payload preserves any unrepresentable source."""
    result = dataframe.copy(deep=False)
    bad_rows = pd.Series(False, index=result.index)
    lineage = {'batch_id', 'file_version_id', 'source_row_number'}
    for column, type_name in target_type_names.items():
        if column not in result or column in lineage:
            continue
        original = result[column]
        lower = type_name.lower()
        if INTEGER_TYPES.search(lower):
            value = pd.to_numeric(original, errors='coerce')
            bound = 2**63
            bad = original.notna() & (value.isna() | ~np.isfinite(value) | (value % 1 != 0) | (value < -bound) | (value >= bound))
        elif 'timestamp' in lower:
            value = pd.to_datetime(original, errors='coerce')
            bad = original.notna() & value.isna()
        elif 'double' in lower or 'float' in lower:
            value = pd.to_numeric(original, errors='coerce')
            bad = original.notna() & (value.isna() | ~np.isfinite(value))
        else:
            continue
        result[column] = value.mask(bad)
        bad_rows |= bad
    if bad_rows.any():
        if 'raw_payload' not in target_type_names:
            raise ValueError('Raw cast failure requires reviewed raw_payload migration; source remains intact')
        result['raw_payload'] = None
        source_columns = [c for c in dataframe if c not in {
            'batch_id','file_version_id','source_file','source_year_month','source_file_sha256',
            'source_row_number','source_record_key','schema_fingerprint','schema_version','source_last_modified'}]
        for index in result.index[bad_rows]:
            result.at[index,'raw_payload'] = {c: _json_value(dataframe.at[index,c]) for c in source_columns}
    return result


class StageWriter:
    """One process-local connection and schema cache per invocation.

    Upsert commits each part as before. Direct writes require an outer transaction
    owned by the atomic staging coordinator; never auto-fallback on errors.
    """
    def __init__(self, serialization='arrow', engine=None, storage_mode='wide', control_schema='control', batch_id=None):
        if serialization not in ('pandas','arrow'):
            raise ValueError('Unknown CSV serialization')
        if storage_mode not in ('wide','slim'): raise ValueError('Unknown stage storage mode')
        self.storage_mode=storage_mode
        self.control_schema=_safe_identifier(control_schema)
        self.batch_id=None if batch_id is None else int(batch_id)
        self.targets={}
        self.file_versions={}
        self.serialization = serialization
        self.engine = engine
        self.owns_engine = engine is None
        self.columns = {}
        self.connection = None

    def __enter__(self):
        self.engine = self.engine or postgres_engine()
        try:
            with phase('connect'):
                self.connection = self.engine.connect()
        except BaseException:
            if self.owns_engine:
                self.engine.dispose()
            raise
        return self

    def __exit__(self, *exception):
        try:
            if self.connection is not None:
                self.connection.close()
        finally:
            if self.owns_engine: self.engine.dispose()

    def metadata(self, schema, table):
        key = (schema, table)
        if key not in self.columns:
            with phase('schema_inspection',schema=schema,table=table):
                self.columns[key] = {c['name']:c for c in inspect(self.connection).get_columns(table,schema=schema)}
        return self.columns[key]

    def target(self,schema,table):
        if table not in ('yellow_taxi_raw','yellow_taxi_canonical'): return table
        key=(schema,table)
        if key not in self.targets:
            kind=self.connection.execute(text("SELECT c.relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname=:schema AND c.relname=:table"),{'schema':schema,'table':table}).scalar()
            if self.storage_mode=='slim':
                mode=self.connection.execute(text(f'SELECT storage_mode FROM "{schema}".stage_storage_contract')).scalar()
                if mode!='slim-v1' or kind!='v': raise ValueError('Slim storage contract/view is missing')
                self.targets[key]=table+'_data'
            else:
                if kind=='v': raise ValueError('Staging view requires explicit slim storage mode; no implicit fallback')
                self.targets[key]=table
        return self.targets[key]

    def project_slim(self,dataframe):
        fields=['batch_id','source_file','source_year_month','source_file_sha256','schema_fingerprint','schema_version','source_last_modified']
        if not set(fields+['file_version_id']).issubset(dataframe.columns):
            raise ValueError('Slim input must retain complete lineage before projection')
        ids=dataframe['file_version_id'].drop_duplicates().tolist()
        if len(ids)!=1: raise ValueError('Slim COPY requires one file/version per frame')
        version=int(ids[0])
        if version not in self.file_versions:
            self.file_versions[version]=self.connection.execute(text(f'SELECT v.*, f.source_last_modified AS attempt_last_modified FROM {self.control_schema}.source_file_version v JOIN {self.control_schema}.etl_file f USING(file_version_id) WHERE v.file_version_id=:version AND f.batch_id=:batch AND f.source_file=v.source_file'),{'version':version,'batch':self.batch_id}).mappings().first()
        metadata=self.file_versions[version]
        if metadata is None: raise ValueError('Missing immutable file version')
        for column in fields:
            original=dataframe[column]
            value=self.batch_id if column=='batch_id' else metadata['attempt_last_modified'] if column=='source_last_modified' else metadata[column]
            if column=='batch_id' and value is None: raise ValueError('Slim writer requires explicit batch identity')
            if column=='source_last_modified':
                cast=pd.to_datetime(original,errors='coerce',utc=True)
                expected=pd.to_datetime(value,utc=True)
                bad=(original.notna() & cast.isna()) | (cast.notna() if pd.isna(expected) else (cast.isna() | cast.ne(expected)))
            else:
                bad=original.isna() | original.ne(value)
            if bad.any(): raise ValueError('Slim lineage mismatch before projection: '+column)
        return dataframe.drop(columns=fields)

    def serialize(self, dataframe, schema, table, conflict_column):
        physical=self.target(schema,table)
        if physical!=table:
            dataframe=self.project_slim(dataframe)
        columns = self.metadata(schema,physical)
        types = {c:str(v['type']) for c,v in columns.items()}
        labels = {'schema':schema,'table':table,'rows':len(dataframe)}
        with phase('cast_frame', **labels):
            source = prepare_raw_frame(dataframe,types) if table=='yellow_taxi_raw' else dataframe
            common = [c for c in source if c in columns]
            required = {c for c,v in columns.items() if not v['nullable'] and v.get('default') is None and not v.get('identity')}
            unknown = set(source)-set(columns)
            if unknown or required-set(common) or conflict_column not in common:
                raise ValueError('Staging column contract mismatch: unknown={}, missing={}'.format(sorted(unknown),sorted(required-set(common))))
            frame = normalize_copy_frame(source[common],types)
        with phase('json_serialization', **labels):
            for column in common:
                if 'json' in types[column].lower():
                    frame[column] = frame[column].map(serialize_json_value)
        with phase('csv_serialization', serialization=self.serialization, **labels):
            if self.serialization == 'arrow':
                import pyarrow as pa
                import pyarrow.csv as csv
                from io import BytesIO
                buffer = BytesIO()
                csv.write_csv(pa.Table.from_pandas(frame,preserve_index=False),buffer,
                              write_options=csv.WriteOptions(include_header=False,batch_size=65536))
                null_marker = ''  # Arrow quotes empty strings; NULL is an unquoted empty field.
            else:
                null_marker = 'm' + uuid.uuid4().hex[:15]
                if any(frame[c].eq(null_marker).fillna(False).any() for c in common):
                    raise ValueError('COPY null marker collision')
                buffer = StringIO()
                frame.to_csv(buffer,index=False,header=False,na_rep=null_marker,lineterminator='\n')
            buffer.seek(0)
        return buffer, common, null_marker

    def write(self,dataframe,schema,table,conflict_column='source_record_key',metrics=None,direct=False):
        if dataframe.empty: return 0
        schema,table,conflict_column = map(_safe_identifier,(schema,table,conflict_column))
        physical=self.target(schema,table)
        if physical!=table and not direct: raise ValueError('Slim views require atomic direct COPY')
        if direct and not self.connection.in_transaction():
            raise RuntimeError('Direct COPY requires an outer atomic transaction')
        labels = {'schema':schema,'table':table,'rows':len(dataframe),'mode':'direct' if direct else 'upsert'}
        buffer,common,null_marker = self.serialize(dataframe,schema,table,conflict_column)
        quoted = ', '.join('"{}"'.format(_safe_identifier(c)) for c in common)
        transaction = None if direct else self.connection.begin()
        temporary = '_mage_{}_{}'.format(table,uuid.uuid4().hex[:12])
        try:
            if not direct:
                with phase('create_temporary',**labels):
                    self.connection.execute(text('CREATE TEMP TABLE "{}" ON COMMIT DROP AS SELECT {} FROM "{}"."{}" WITH NO DATA'.format(temporary,quoted,schema,table)))
            target = '"{}"."{}"'.format(schema,physical) if direct else '"{}"'.format(temporary)
            with phase('copy',**labels):
                with self.connection.connection.cursor() as cursor:
                    cursor.copy_expert("COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '{}')".format(target,quoted,null_marker),buffer)
            if direct:
                inserted = len(dataframe)  # COPY aborts the transaction on any rejected row.
            else:
                with phase('conflict_validation',**labels):
                    duplicates=self.connection.execute(text('SELECT 1 FROM "{}" GROUP BY "{}" HAVING count(*)>1 LIMIT 1'.format(temporary,conflict_column))).first()
                    if duplicates: raise ValueError('Duplicate keys in input chunk')
                    comparisons=' OR '.join('d."{0}" IS DISTINCT FROM s."{0}"'.format(c) for c in common)
                    mismatch=self.connection.execute(text('SELECT 1 FROM "{}" s JOIN "{}"."{}" d ON d."{}"=s."{}" WHERE {} LIMIT 1'.format(temporary,schema,table,conflict_column,conflict_column,comparisons))).first()
                    if mismatch: raise ValueError('Conflicting source key with different payload or scope')
                with phase('insert',**labels):
                    inserted=self.connection.execute(text('INSERT INTO "{}"."{}" ({}) SELECT {} FROM "{}" ON CONFLICT ("{}") DO NOTHING'.format(schema,table,quoted,quoted,temporary,conflict_column))).rowcount
                with phase('commit',**labels): transaction.commit()
        except BaseException:
            if transaction is not None: transaction.rollback()
            raise
        finally:
            buffer.close()
        counts={'attempted':len(dataframe),'inserted':inserted,'conflicted':len(dataframe)-inserted}
        if metrics is not None:
            for k,v in counts.items(): metrics[k]=metrics.get(k,0)+v
        # Direct counts are tentative until the outer COMMIT; caller discards them on failure.
        print('MAGE_WRITE '+json.dumps({**labels,**counts,'committed':not direct}),flush=True)
        return len(dataframe)

    def write_stream(self, frames, schema, table, conflict_column='source_record_key',
                     metrics=None, on_part=None):
        """COPY a frame iterator with one PostgreSQL COPY command.

        Each frame is still bounded in memory and independently normalized and
        serialized. PostgreSQL sees one continuous stream, eliminating one
        protocol/statement round trip per source chunk.
        """
        schema, table, conflict_column = map(
            _safe_identifier, (schema, table, conflict_column)
        )
        if not self.connection.in_transaction():
            raise RuntimeError('Streaming COPY requires an outer atomic transaction')
        iterator = iter(frames)
        try:
            first_frame, first_details = next(iterator)
        except StopIteration:
            return 0
        if first_frame.empty:
            raise ValueError('Slim-v2 COPY received an empty first frame')
        first_buffer, common, null_marker = self.serialize(
            first_frame, schema, table, conflict_column
        )
        total = len(first_frame)
        parts = 1
        if on_part is not None:
            on_part(parts, first_details)

        def buffers():
            nonlocal total, parts
            yield first_buffer
            for frame, details in iterator:
                if frame.empty:
                    raise ValueError('Slim-v2 COPY received an empty frame')
                buffer, columns, marker = self.serialize(
                    frame, schema, table, conflict_column
                )
                if columns != common or marker != null_marker:
                    buffer.close()
                    raise ValueError('Slim-v2 COPY frame contract changed mid-stream')
                total += len(frame)
                parts += 1
                if on_part is not None:
                    on_part(parts, details)
                yield buffer

        quoted = ', '.join('"{}"'.format(_safe_identifier(c)) for c in common)
        target = '"{}"."{}"'.format(schema, table)
        reader = _CopyBufferChain(buffers())
        labels = {'schema': schema, 'table': table, 'mode': 'direct-stream'}
        try:
            with phase('copy_stream', **labels):
                with self.connection.connection.cursor() as cursor:
                    cursor.copy_expert(
                        "COPY {} ({}) FROM STDIN WITH (FORMAT CSV, NULL '{}')".format(
                            target, quoted, null_marker
                        ),
                        reader,
                    )
        finally:
            reader.close()
        counts = {'attempted': total, 'inserted': total, 'conflicted': 0}
        if metrics is not None:
            for key, value in counts.items():
                metrics[key] = metrics.get(key, 0) + value
        print(
            'MAGE_WRITE ' + json.dumps({**labels, 'parts': parts, **counts, 'committed': False}),
            flush=True,
        )
        return total


def upsert_dataframe(dataframe,schema,table,conflict_column='source_record_key',metrics=None,writer=None):
    if dataframe.empty: return 0
    if writer is not None:
        return writer.write(dataframe,schema,table,conflict_column,metrics)
    with StageWriter() as session:
        return session.write(dataframe,schema,table,conflict_column,metrics)
