from sqlmodel import Field, create_engine
from pathlib import Path
import os
from fsspec.implementations.local import LocalFileSystem

from . import LakeEntry, CASLake

class Migrated(LakeEntry, table=True):
    path: str = Field(primary_key=True)

def migrate_tree(where, where_from):
    os.makedirs(where, exist_ok=True)
    lake = CASLake(
            engine=create_engine(f'sqlite:///{where}/catalog.sqlite'),
            filesystem=LocalFileSystem(auto_mkdir=True),
            base_dir=f'{where}/store',
    )

    orig = Path(where_from)

    with lake.transaction() as t:
        for file in orig.glob('**/*'):
            if not file.is_file():
                continue
            with file.open('rb') as fo:
                t.put(fo, Migrated(path=str(file.relative_to(orig)), filetype=file.suffix))
        
        t.commit()
