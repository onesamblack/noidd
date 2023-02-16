import xxhash
import asyncio
from aiofiles import AIOFile, Reader



def check_leveldb(self, filename: str) -> str:
        """returns the hash for the filename from the db instance

        **Note**: this is a blocking task

        Parameters
        ----------
        filename : str
            filename
        """
        cs = self.db.get(filename)
        return cs

def add_checksum_to_leveldb(self, filename: str, checksum: str):
        """adds a checksum to the db

        Parameters
        ----------
        filename : str
            filename
        checksum : str
            checksum
        """
        self.db.put(str(filename).encode(), str(checksum).encode())




def get_db(*args, **kwargs) -> Sequence[plyvel.PrefixedDB]:
    """get_db.

    Parameters
    ----------
    args :
        args
    kwargs :
        kwargs

    Returns
    -------
    Sequence[plyvel.PrefixedDB]

    """
    db_filename = config["db_file"]
    db = plyvel.DB(db_filename, **config["db_options"])

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



