import xxhash
import re
import struct
import asyncio
import plyvel
import pytz
import aiofiles.os
from datetime import datetime
from aiofile import AIOFile, Reader
from typing import Union, Sequence, Callable, Optional, Any
from functools import wraps


UTC = pytz.utc
Eastern = pytz.timezone("US/Eastern")




def timestring(timestamp):
    """
    all timezones reported are utc - convert them to eastern
    """
    dt = datetime.fromtimestamp(timestamp, UTC)
    eastern = dt.astimezone(Eastern)
    return eastern.strftime("%Y-%m-%d %H:%M:%S")


def float_encoder(val: float) -> bytes:
    buf = bytearray(8)
    v = struct.pack_into("f", buf, 0, val)
    return bytes(buf)


def float_decoder(val: bytes) -> float:
    v = struct.unpack_from("f", val)
    return v[0]


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
        is_dir = await aiofiles.os.path.isdir(filepath)
        if not any([is_link, is_dir]):
            return (filepath, False)
        else:
            if is_link:
                link = await aiofiles.os.readlink(filepath)
                return (link, False)
            if is_dir:
                return (filepath, True)
    return (None, False)


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
    db: plyvel.DB, key: str, decoder: Optional[Callable[[bytes], Any]] = None
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
    if not decoder:
        decoder = utf8_str_decoder
    result = await asyncio.to_thread(db.get, key.encode("utf-8"))
    if result:
        value = decoder(result)
        return value


async def leveldb_aput(
    db: plyvel.DB, key: str, value: Any, encoder: Callable[[Any], bytes] = None
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
    if not encoder:
        encoder = utf8_str_encoder
    encoded_value = encoder(value)
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
        self,
        db: Union[plyvel.DB, plyvel._plyvel.PrefixedDB,],
        include: Sequence[str] = [],
        exclude: Sequence[str] = [],
        prefix: Optional[str] = None
    ):
        """__init__.

        Parameters
        ----------
        db : plyvel.DB
            db
        kwargs :
            kwargs
        """
        self.include = []
        self.exclude = []
        if type(db) == plyvel._plyvel.PrefixedDB:
            self.db = db.db
            self.include.append(self.make_prefix_regex(db.prefix.decode("utf-8")))
        
        elif type(db) in [plyvel._plyvel.DB, plyvel._plyvel.Snapshot]:
            self.db = db

        if prefix:
            self.include.append(self.make_prefix_regex(prefix))

        for p in include:
            self.include.append(re.compile(p))
        for p in exclude:
            self.exclude.append(re.compile(p))
        self.iter_ = self.db.raw_iterator()
        self.iter_.seek_to_first()
    
    def make_prefix_regex(self, prefix):
        return re.compile(prefix + ".+")

    def valid(self):
        """valid."""
        return self.iter_.valid()

    def match(self):
        """
        files foo loo hoz haz cheez
        .+o.+, .+a.+
        f.+
        """
        key = self.iter_.key().decode("utf-8")
        match_include = True
        match_exclude = False
        if self.include:
            if not all([p.match(key) for p in self.include]):
                match_include = False
        if self.exclude:
            if any([p.match(key) for p in self.exclude]):
                match_exclude = True
        return match_include and not match_exclude

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
