import pylyvel
import asyncio
import aiofiles
from utils import leveldb_aput, leveldb_aget, leveldb_adelete, AsyncLevelDBIterator
from typing import Optional, Sequence, Union

class Watcher:
    """
    Watchers compute checksums for all the files in their watched directories.

    When they finish their process - a list of ```NoidNotifications``` is returned.
    """

    def __init__(
        self,
        db: pylyvel.PrefixedDB,
        initialized:bool,
        notifiers: Sequence[Notifier],
        glob: Optional[str] = None,
        filelist: Optional[Sequence]=None,
        root_dir: Optional[str] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        """__init__.

        Parameters
        ----------
        glob : str
            a pattern as used by glob, ex /mydir/*.cc
        db : pylyvel.PrefixedDB
            an instance of pylyvel.PrefixedDB 
        notifier : Union[Notifier,Sequence[Notifier]]
            either a list of notifiers, or a single notifier
        loop : Optional[asyncio.AbstractEventLoop]
            loop
        """
        self._loop = loop or asyncio.get_event_loop()
        self.db = db
        self.initialized = initialized
        self.notifiers = notifiers
        self.glob = glob
        self.filelist = filelist
        self.root_dir = root_dir
        self.snapshot = self.db.snapshot()
        self.checksums = asyncio.Queue()
        for n in self.notifiers:
            n.add_watcher()

    async def notify_all(self, type_:str, **kwargs):
        coros = []
        for notifier in self.notifiers:
            coros.append(notifier.notify,type_=type_, **kwargs) 
        await asyncio.gather(*coros)

    async def verify_checksums(self):
        while True:
            messages = []
            f = await self.checksums.get()
            if not self.initialized:
                # running for the first time
                await leveldb_aput(db=self.db, key=f[0], value=f[1])
            else:
                cs = await utils.leveldb_aget(db=self.db, key=f[0])
                if cs is None:
                    # the checksum didn't exist - new file
                    # create a notification
                    stat = await aiofiles.os.stat(str(f[0]))
                    st_time = stat.st_time
                    await self.notify_all(type_="created", f=f[0], t=st_time)
                    # add it 
                    await leveldb_aput(db=self.db, key=f[0], value=f[1])
                else:
                    if cs != f[1]:
                        # the checksum didn't match
                        stat = await aiofiles.os.stat(str(f[0]))
                        st_time = stat.st_time
                        await self.notify_all(type_="modified", f=f[0], t=st_time)
                        # add it 
                        await leveldb_aput(db=self.db, key=f[0], value=f[1])
                if f is None:
                    # no more files to check
                    notif = self.notify_all(type_="done")
                    self.checksums.task_done()
                    break

            self.fs_checksums.task_done()
        if not self.initialized:
            # add an intialized timestamp
            await leveldb_adelete(db=self.db, key="initialized", value=time.time())
    async def get_current_checksums(self):
        """get_fs_checksums.
        """
        async for f in asyncpath.rglob(self.file_glob):
            cs = await xxsum(f)
            self.checksums.put_nowait((f, cs))
        await self.fs_checksums.put_nowait(None)

    async def get_watched_files(self):
        """
        gets all existing watched files from the level db instance and checks them against
        the filesystem
        """

        async for item in AsyncLevelDBIterator(db=self.snapshot):
            f = item[0]
            exists = await aiofiles.os.exists(f)
            if not exists:
                # the file was deleted/moved
                await self.notify_all(type_="deleted", f=f[0])


    async def run(self):
        """run.
        """
        await asyncio.gather(self.get_watched_files(), 
                             self.get_current_checksums(), 
                             self.verify_checksums())
