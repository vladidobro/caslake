from sqlmodel import create_engine
from pathlib import Path
from fsspec.implementations.local import LocalFileSystem
from typing import Callable

from . import LakeEntry, CASLake

def make_symlink_tree(where, where_from, select, model_to_path: Callable[[LakeEntry], str]):
    lake = CASLake(
            engine=create_engine(f'sqlite:///{where_from}/catalog.sqlite'),
            filesystem=LocalFileSystem(auto_mkdir=True),
            base_dir=f'{where_from}/store',
    )
    where = Path(where)

    entries = lake.select(select)
    symlinks = [(lake.to_path(entry), model_to_path(entry)) for entry in entries]

    # delete old
    for f in where.glob('**/*'):
        if f.is_symlink():
            f.unlink()
            continue
        if not f.is_dir():
            raise RuntimeError(f'{f} is not symlink!')
    if not all(f.is_dir() for f in where.glob('**/*')):
        raise RuntimeError('Only dirs should be left now')
    for f in where.glob('*'):
        f.unlink()

    # create symlinks
    for target, source in symlinks:
        (where/source).symlink_to(target)
