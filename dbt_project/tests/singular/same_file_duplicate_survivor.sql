select loser.source_record_key
from {{ ref('duplicate_quarantine') }} loser
left join {{ ref('int_taxi_trip_classified') }} winner
    on winner.source_record_key = loser.duplicate_survivor_source_record_key
where loser.duplicate_occurrence_number <= 1
   or winner.source_record_key is null
   or winner.source_file_sha256 <> loser.source_file_sha256
   or winner.raw_exact_fingerprint <> loser.raw_exact_fingerprint
   or winner.source_row_number >= loser.source_row_number
   or winner.duplicate_occurrence_number <> 1
