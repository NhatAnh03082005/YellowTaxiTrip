from pathlib import Path, PurePosixPath


CONTAINER_DATA_ROOT = Path('/home/src/data')


def resolve_data_path(catalog_path, data_root=None):
    """Resolve a portable catalog path below the mounted data directory."""
    root = Path(data_root or CONTAINER_DATA_ROOT).resolve()
    normalized = str(catalog_path).replace('\\', '/')
    parts = list(PurePosixPath(normalized).parts)
    if parts and parts[0].lower() == 'data':
        parts = parts[1:]
    resolved = root.joinpath(*parts).resolve()
    resolved.relative_to(root)
    return resolved
