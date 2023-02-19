import xxhash
import re
import struct
import asyncio
import plyvel
import aiofiles
import threading
from aiofile import AIOFile, Reader
from typing import Sequence, Callable, Optional, Any


def float_encoder(val: float) -> bytes:
    v = struct.pack("f", val)
    return v


def float_decoder(val: bytes) -> float:
    v = struct.unpack("f", val)
    return v


def utf8_str_encoder(val: str) -> bytes:
    return val.encode("utf-8")


def utf8_str_decoder(val: bytes) -> str:
    return val.decode("utf-8")


async def checkfile(filepath) -> str:
    """checks for the existence of the file - if it exists,
    checks to see if it's a symlink - if true, returns the
    linked file

    Parameters
    ----------
    filepath :
        filepath

    Returns
    -------
    str

    """
    exists = await aiofiles.os.path.exists(filepath)
    if exists:
        is_link = await aiofiles.os.path.islink(filepath)
        if not is_link:
            return filepath
        else:
            # followsymlink
            link = await aiofiles.os.readlink(filepath)
            return link
    return None


async def xxsum(filename: str) -> str:
    """calculates a filehash using xxHash

    Parameters
    ----------
    filename : str
        filename

    Returns
    -------
    str
        the filehash

    """
    h = xxhash.xxh64()
    async with AIOFile(filename, "rb") as f:
        reader = Reader(f, chunk_size=512)
        async for chunk in reader:
            h.update(chunk)
    return h.hexdigest()


async def leveldb_aget(
    db: plyvel.DB, key: str, decoder: Optional[Callable[[bytes], str]] = None
) -> str:
    """leveldb_aget.

    Parameters
    ----------
    db : plyvel.DB
        db
    key : str
        key

    Returns
    -------
    str

    """
    print("called aget")
    if not decoder:
        decoder = utf8_str_decoder
    result = await asyncio.to_thread(db.get, key.encode("utf-8"))
    if result:
        try:
            result = decoder(result)
        except Exception as e:
            print(e)
    return result


async def leveldb_aput(
    db: plyvel.DB, key: str, value: Any, encoder: Callable[[Any], bytes]=None
):
    """leveldb_aput.

    Parameters
    ----------
    db : plyvel.DB
        db
    key : str
        key
    value : str
        value
    """
    print("called aput")
    if not encoder:
        encoder = utf8_str_encoder
    encoded_value = encoder(value)
    print(f"encoded value {encoded_value}")
    res = await asyncio.to_thread(db.put, key.encode("utf-8"), encoded_value)
    return res

async def leveldb_adelete(db: plyvel.DB, key: str):
    """leveldb_adelete.

    Parameters
    ----------
    db : plyvel.DB
        db
    key : str
        key
    """

    res = await asyncio.to_thread(db.delete, key.encode("utf-8"), sync=True)
    return res
class AsyncLevelDBIterator:
    """
    see https://plyvel.readthedocs.io/en/latest/api.html#RawIterator
    for available kwargs
    """

    def __init__(
        self, db: plyvel.DB, include:Sequence[str]=[], exclude:Sequence[str]=[], **kwargs
    ):
        """__init__.

        Parameters
        ----------
        db : plyvel.DB
            db
        kwargs :
            kwargs
        """
        self.db = db
        self.iter_ = db.raw_iterator(**kwargs)
        self.iter_.seek_to_first()
        self.include = []
        self.exclude = []
        for p in include:
            self.include.append(re.compile(p))
        for p in exclude:
            self.exclude.append(re.compile(p))

    def valid(self):
        """valid."""
        return self.iter_.valid()

    def match(self):
        key = self.iter_.key().decode("utf-8")
        match_include = True
        match_exclude = False
        if self.include:
            if not any([p.match(key) for p in self.include]):
                match_include = False
        if self.exclude:
            if any([p.match(key) for p in self.exclude]):
                match_exclude = True
        return match_include & (not match_exclude)

    def __aiter__(self):
        """__aiter__."""
        return self

    async def __anext__(self):
        """__anext__."""
        try:
            current_item = self.iter_.item()
            m = self.match()
            self.iter_.next()
            if m:
                return current_item
        except plyvel._plyvel.IteratorInvalidError:
            raise StopAsyncIteration


