import xxhash
import struct
import asyncio
import plyvel
from aiofile import AIOFile, Reader

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

async def leveldb_aget(db:plyvel.DB, 
                       key: str) -> str:
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
    v = await asyncio.to_thread(db.get(key.encode("utf-8")))
    return v

async def leveldb_aput(db:plyvel.DB,
                       key: str, 
                       value: str):
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
       
    if type(value) == float:
        value = struct.pack("f", value)
    else:
        value = value.encode("utf-8")
    await asyncio.to_thread(db.put(key.encode("utf-8"), value))

async def leveldb_adelete(db:plyvel.DB,
        key: str):
    """leveldb_adelete.

    Parameters
    ----------
    db : plyvel.DB
        db
    key : str
        key
    """
       
    await asyncio.to_thread(db.delete(key.encode("utf-8"), sync=True))


class AsyncLevelDBIterator: 
    """
    see https://plyvel.readthedocs.io/en/latest/api.html#RawIterator
    for available kwargs
    """
    def __init__(self, db:plyvel.DB, **kwargs):
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
        """valid.
        """
        return self.iter_.valid() 
    def __aiter__(self):
        """__aiter__.
        """
        return self
    async def __anext__(self):
        """__anext__.
        """
        try:
            current_item = self.iter_.item()
            self.iter_.next()
            return current_item
        except plyvel._plyvel.IteratorInvalidError:
            raise StopAsyncIteration
