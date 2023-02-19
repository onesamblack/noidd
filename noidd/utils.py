import xxhash
import struct
import asyncio
import plyvel
import aiofiles
from aiofile import AIOFile, Reader


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


async def leveldb_aget(db: plyvel.DB, key: str) -> str:
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
    await asyncio.to_thread(db.get, key.encode("utf-8"))
    if v:
        return v.decode("utf-8")


async def leveldb_aput(db: plyvel.DB, key: str, value: str):
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
    if type(value) == float:
        value = struct.pack("f", value)
    else:
        value = value.encode("utf-8")
    print("ok")
    await asyncio.to_thread(db.put, key.encode("utf-8"), value)


async def leveldb_adelete(db: plyvel.DB, key: str):
    """leveldb_adelete.

    Parameters
    ----------
    db : plyvel.DB
        db
    key : str
        key
    """

    await asyncio.to_thread(db.delete, key.encode("utf-8"), sync=True)


class AsyncLevelDBIterator:
    """
    see https://plyvel.readthedocs.io/en/latest/api.html#RawIterator
    for available kwargs
    """

    def __init__(self, db: plyvel.DB, **kwargs):
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

    def valid(self):
        """valid."""
        return self.iter_.valid()

    def __aiter__(self):
        """__aiter__."""
        return self

    async def __anext__(self):
        """__anext__."""
        try:
            current_item = self.iter_.item()
            self.iter_.next()
            return current_item
        except plyvel._plyvel.IteratorInvalidError:
            raise StopAsyncIteration
