import plyvel
import asyncio
import aiofiles
import aiopath
import logging
from concurrent.futures import ThreadPoolExecutor
from notifiers import Notifier
from utils import (
    xxsum,
    checkfile,
    leveldb_aput,
    leveldb_aget,
    leveldb_adelete,
    AsyncLevelDBIterator,
)
from typing import Optional, Sequence, Union


class Watcher:
    """
    Watchers compute checksums for all the files in their watched directories.

    When they finish their process - a list of ```NoidNotifications``` is returned.
    """

    def __init__(
        self,
        name: str,
        db: plyvel.DB,
        initialized: bool,
        notifiers: Sequence[Notifier],
        glob: Optional[str] = None,
        filelist: Optional[Sequence] = None,
        root_dir: Optional[str] = None,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        """__init__.

        Parameters
        ----------
        glob : str
            a pattern as used by glob, ex /mydir/*.cc
        db : plyvel.DB
            an instance of plyvel.DB
        notifier : Union[Notifier,Sequence[Notifier]]
            either a list of notifiers, or a single notifier
        loop : Optional[asyncio.AbstractEventLoop]
            loop
        """
        self.name = name
        self._loop = loop or asyncio.get_event_loop()
        self.db = db
        self.initialized = initialized
        self.notifiers = notifiers
        self.glob = glob
        self.filelist = filelist
        self.root_dir = root_dir
        self.snapshot = self.db.snapshot()
        self.checksums = asyncio.Queue()
        self.executor = ThreadPoolExecutor(max_workers=2)
        for n in self.notifiers:
            n.add_watcher()

    async def notify_all(self, type_: str, **kwargs):
        print("notifying all")
        coros = []
        for notifier in self.notifiers:
            coros.append(notifier.notify, type_=type_, **kwargs)
        return await asyncio.gather(*coros)

    async def verify_checksums(self):
        print("starting to verify checksums")
        while True:
            messages = []
            f = await self.checksums.get()
            self.checksums.task_done()
            print(f"got checksum from queue {f[0]}:{f[1]}")
            if not self.initialized:
                # running for the first time
                print(f"submitting {f[0]} -first time {self.checksums.qsize()}")
                await leveldb_aput(db=self.db, key=f[0], value=f[1])
                print("added f[0] to db")
                self.checksums.task_done()
            else:
                cs = await leveldb_aget(db=self.db, key=f[0])
                if cs is None:
                    # the checksum didn't exist - new file
                    # create a notification
                    stat = await aiofiles.os.stat(str(f[0]))
                    st_time = stat.st_time
                    await self.notify_all(type_="created", f=f[0], t=st_time)
                    # add it
                    await leveldb_aput(db=self.db, key=f[0], value=f[1])
                    self.checksums.task_done()

                elif cs != f[1]:
                    # the checksum didn't match
                    stat = await aiofiles.os.stat(str(f[0]))
                    st_time = stat.st_time
                    await self.notify_all(type_="modified", f=f[0], t=st_time)
                    # add it
                    await leveldb_aput(db=self.db, key=f[0], value=f[1])
                    self.checksums.task_done()
                else:
                    # checksum matched
                    print(f"matched checksum {f[0]}+{f[1]} cs:{cs}")
                    self.checksums.task_done()
            if f is None:
                # no more files to check
                print("done")
                await self.notify_all(type_="done")
                break

        if not self.initialized:
            # add an intialized timestamp
            await leveldb_aput(db=self.db, key="initialized", value=time.time())
        print("completed verifying checksums")

    async def get_current_checksums(self):
        """get_fs_checksums."""
        print("starting get current checksums")
        if self.filelist:
            for f in self.filelist:
                file_ = await checkfile(f)
                print(f"{file_}")
                if not file_:
                    print(f"a file: {f} to be watched doesn't exist on the filesystem")
                else:
                    cs = await xxsum(file_)
                    self.checksums.put_nowait((f, cs))
                    print(f"put {cs} checksum in queue -getcurrent")
                    print(self.checksums.qsize())
        else:
            # run the loop for file globs or root dir
            ptn = self.glob if self.glob else "*"
            print(ptn)
            async for f in aiopath.AsyncPath(self.root_dir).glob(ptn):
                file_ = await checkfile(f)
                cs = await xxsum(file_)
                self.checksums.put_nowait((f, cs))
                print(f"put checksum {cs} in queue -getcurrent")
                print(self.checksums.qsize())
            await self.checksums.put_nowait(None)
        print("completed current checksums")

    async def get_watched_files(self):
        """
        gets all existing watched files from the level db instance and checks them against
        the filesystem
        """
        print("starting get_watched")
        async for item in AsyncLevelDBIterator(db=self.snapshot):
            print(f"{item[0]}")
            f = item[0]
            file_ = await checkfile(f)
            if not file_:
                # the file was deleted/moved
                print(f"delete message for {f[0]}")
                await self.notify_all(type_="deleted", f=f[0])
                # delete the hash from the db
                await leveldb_adelete(db=self.db, key=f[0])
        print("finished get watched")
        return

    async def run(self):
        """run."""
        await asyncio.gather(
            *[
                self.get_watched_files(),
                self.get_current_checksums(),
                self.verify_checksums(),
            ],
            return_exceptions=True,
        )
