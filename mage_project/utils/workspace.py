"""Shared workspace configuration; never removes pending or unverified batches."""
import os
import shutil
from pathlib import Path


def work_root(value=None):
    return Path(value or os.environ.get('MAGE_WORK_ROOT','/home/src/data/work/task_4_4')).resolve()


def batch_directory(root, batch_id):
    batch_id=int(batch_id)
    if batch_id<=0: raise ValueError('Positive batch_id required')
    root=Path(root).resolve()
    path=root/str(batch_id)
    if path.is_symlink() or path.resolve()!=path:
        raise ValueError('Workspace batch path redirects elsewhere')
    path.relative_to(root)
    return path


def check_space(root, reserve_bytes=512*1024*1024):
    root=Path(root).resolve()
    root.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(root).free<int(reserve_bytes):
        raise RuntimeError('Insufficient workspace disk headroom')


def artifact_bytes(root):
    root=Path(root)
    return sum(p.stat().st_size for p in root.rglob('*') if p.is_file() and not p.is_symlink()) if root.exists() else 0


def owned_child(batch,name):
    if name not in ('raw','canonical','technical_reject'):
        raise ValueError('Unknown workspace artifact directory')
    path=Path(batch)/name
    if path.is_symlink() or path.resolve()!=path:
        raise ValueError('Workspace artifact path redirects elsewhere')
    return path


def check_budget(root):
    check_space(root)
    limit=int(os.environ.get('MAGE_WORK_LIMIT_BYTES',str(3*1024**3)))
    if limit<=0: raise ValueError('Positive workspace byte budget required')
    if artifact_bytes(root)>limit:
        raise RuntimeError('Workspace budget exceeded; preserve pending artifacts and finish approved cleanup')
