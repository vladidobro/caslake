from . import CASLake, LakeEntry
from sqlmodel import Field, create_engine, select
from io import BytesIO
from fsspec.implementations.local import LocalFileSystem
import os
from pathlib import Path


os.system('rm -rf ./data/*')
os.makedirs('./data', exist_ok=True)
Path('./data/.gitignore').write_text('*')

lake = CASLake(
        engine=create_engine('sqlite:///data/catalog.sqlite'),
        filesystem=LocalFileSystem(auto_mkdir=True),
        base_dir='./data/store',
)

class File(LakeEntry, table=True):
    name: str = Field(primary_key=True)

File.metadata.create_all(lake.engine)

with lake.transaction() as t:
    for i in range(10):
        t.put(BytesIO(str(i).encode()), File(name=str(i), filetype='.txt'))

    t.commit()

entries = lake.select(select(File).where(File.name=='3'))
print(entries)

[entry] = entries

with lake.open(entry) as f:
    print(f.read())

print(lake.read_bytes(entry))
print(lake.to_path(entry))
