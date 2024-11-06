from uuid import uuid4, UUID
from hashlib import sha256
from base64 import b32encode
from dataclasses import dataclass
from sqlalchemy import Engine
from fsspec import AbstractFileSystem
from sqlmodel import SQLModel, Session
from typing import BinaryIO
from contextlib import contextmanager

class _HashingReadBuffer:
    def __init__(self, /, buffer):
        self._buffer = buffer
        self._sha = sha256()

    def read(self, n: int = -1):
        data = self._buffer.read(n)
        self._sha.update(data)
        return data

    def digest(self):
        return self._sha.digest()

def path_b32_encode(content: bytes):
    return b32encode(content).rstrip(b'=').lower().decode()

class LakeEntry(SQLModel):
    sha256: str | None = None
    filetype: str


class UnitOfWork:
    def __init__(self, backend):
        self._backend = backend
        self._files = {}

    def put(self, fo: BinaryIO, metadata: LakeEntry):
        uuid = uuid4()
        self._backend._put(fo, uuid, metadata.filetype)
        self._files[uuid] = metadata

    def commit(self):
        shas = self._backend._flush(list(self._files.keys()))
        for uuid, metadata in self._files.items():
            metadata.sha256 = shas[uuid]
        self._backend._commit(list(self._files.values()))
        self._files = {}

    def rollback(self):
        self._backend._clean(self._files)
        self._files = {}

@dataclass
class CASLake:
    engine: Engine
    filesystem: AbstractFileSystem
    base_dir: str

    def __post_init__(self):
        self._intransaction = False
        self._shas = {}
        self._filetypes = {}
        if self.base_dir.endswith('/') and len(self.base_dir.rstrip('/')):
            self._base_dir = self.base_dir.rstrip('/') + '/'
        else:
            self._base_dir = self.base_dir + '/'

    @contextmanager
    def transaction(self):
        if self._intransaction:
            raise RuntimeError
        uow = UnitOfWork(self)
        self._intransaction = True
        try:
            yield uow
        finally:
            uow.rollback()
            self._intransaction = False

    def _put(self, fo, uuid, filetype):
        hfo = _HashingReadBuffer(fo)
        path = path_b32_encode(uuid.bytes)
        self.filesystem.pipe(self._base_dir + path, hfo.read())
        self._shas[uuid] = path_b32_encode(hfo.digest())
        self._filetypes[uuid] = filetype

    def _flush(self, files: list[UUID]):
        for uuid in files:
            self.filesystem.move(self._base_dir + path_b32_encode(uuid.bytes), self._base_dir + self._shas[uuid] + self._filetypes[uuid])
        return self._shas

    def _clean(self, files: list[UUID]):
        for uuid in files:
            self.filesystem.rm(self._base_dir + path_b32_encode(uuid.bytes))
            self._shas = {}
            self._filetypes = {}

    def _commit(self, metadata: list[LakeEntry]):
        with Session(self.engine) as session:
            for metadatum in metadata:
                session.add(metadatum)
            session.commit()

    def select(self, statement):
        with Session(self.engine) as session:
            return session.exec(statement).all()

    def read_bytes(self, file: LakeEntry):
        return self.filesystem.read_bytes(self.to_path(file))

    def open(self, file: LakeEntry, mode='rb', **kw):
        return self.filesystem.open(self.to_path(file), mode=mode, **kw)

    def to_path(self, file: LakeEntry):
        assert file.sha256 is not None
        return self._base_dir + file.sha256 + file.filetype
