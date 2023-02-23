import plyvel
import time
import asyncio
import aiofiles
import aiopath
import logging
from concurrent.futures import ThreadPoolExecutor
from notifiers import Notifier
from utils import (
    timestring,
    xxsum,
    checkfile,
    leveldb_aput,
    leveldb_aget,
    leveldb_adelete,
    float_encoder,
    float_decoder,
    AsyncLevelDBIterator,
)
from typing import Optional, Sequence, Union

class InteractiveWatcher:
    """
    Watchers compute checksums for all the files in their watched directories.

    When they finish their process - a list of ```NoidNotifications``` is returned.
    """

    def __init__(
        self,
        name: str,
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
        self._loop = loop or asyncio.get_running_loop()
        self.initialized = initialized
        self.notifiers = notifiers
        self.glob = glob
        self.filelist = filelist
        self.root_dir = root_dir
        self.notifications = asyncio.Queue()
        self.inotifys = asyncio.Queue()
        self.files = set()
        self.ignore = [re.compile(".+~")]
    
    async def start(self):
        for n in self.notifiers:
            n.add_watcher()

    async def close(self):
        pass
    
    async def notify_all(self, type_: str, **kwargs):
        # print("notifying all")
        coros = []
        self.notifications += 1
        for notifier in self.notifiers:
            coros.append(notifier.notify(type_=type_, **kwargs))
        await asyncio.gather(*coros)
    async def start_inotify_process(self):
        pass
    async def _main(self):
        """
        wraps the main watcher functions into a task object
        """
        pass
    async def run(self):
        """run."""
        pass
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
        self._loop = loop or asyncio.get_running_loop()
        self.db = db
        self.initialized = initialized
        self.notifiers = notifiers
        self.glob = glob
        self.filelist = filelist
        self.root_dir = root_dir
        self.delete_batch = asyncio.Queue()
        try:
            self.prefix = self.db.prefix.decode("utf-8")
            print(f"initialization {self.prefix}")
        except:
            self.prefix = ""
        self.checksums = asyncio.Queue()
    async def start(self):
        for n in self.notifiers:
            n.add_watcher()
        self.notifications = 0
        self.fscount = 0
        # check for an initstate just in case it was started from a stopped state
        initstate = await leveldb_aget(
            db=self.db, key="initialized", decoder=float_decoder
        )
        if initstate:
            self.initialized = True

    async def close(self):
        print(f"closing {self.name}")
        while True:
            try:
                file_ = self.delete_batch.get_nowait()
                await leveldb_adelete(db=self.db, key=file_)
                self.delete_batch.task_done()
            except asyncio.QueueEmpty:
                break
        if not self.initialized:
            # add an intialized timestamp
            await leveldb_aput(
                db=self.db, key="initialized", value=time.time(), encoder=float_encoder
            )
        self.initialized = True

    async def notify_all(self, type_: str, **kwargs):
        # print("notifying all")
        coros = []
        for notifier in self.notifiers:
            coros.append(await notifier.notify(type_=type_, **kwargs))
        await asyncio.gather(*coros)

    async def verify_checksums(self):
        # print("starting to verify checksums")
        while True:
            f = await self.checksums.get()
            if f != "done":
                #print(f"got checksum from queue {f[0]}:{f[1]}")
                if not self.initialized:
                    # running for the first time
                    #print(f"submitting {f[0]} -first time {self.checksums.qsize()}")
                    await leveldb_aput(db=self.db, key=f[0], value=f[1])
                    #print(f"added {f[0]} to db")
                    self.checksums.task_done()
                else:
                    #print("getting existing keys")
                    cs = await leveldb_aget(db=self.db, key=f[0])
                    #print(f"existing key: {cs} : {f[1]}")
                    if cs is None:
                        # the checksum didn't exist - new file
                        # create a notification
                        self.notifications += 1
                        stat = await aiofiles.os.stat(str(f[0]))
                        ts = stat.st_mtime
                        await self.notify_all(
                            type_="created", f=f[0], t=timestring(ts)
                        )
                        await leveldb_aput(db=self.db, key=f[0], value=f[1])
                        self.checksums.task_done()
                    elif cs != f[1]:
                        # the checksum didn't match
                        self.notifications += 1
                        stat = await aiofiles.os.stat(str(f[0]))
                        ts = stat.st_mtime
                        await self.notify_all(
                            type_="modified", f=f[0], t=timestring(ts)
                        )

                        await leveldb_aput(db=self.db, key=f[0], value=f[1])
                        print(f"added {f[0]} to db")
                        self.checksums.task_done()
                    elif cs == f[0]:
                        # checksum matched
                        #print(f"matched checksum {f[0]}+{f[1]} cs:{cs}")
                        self.checksums.task_done()
            else:
                self.checksums.task_done()
                break

        print("completed verifying checksums")
    async def get_current_checksums(self):
        """get_fs_checksums."""
        print("starting get current checksums")
        if self.filelist:
            for f in self.filelist:
                file_, isdir = await checkfile(f)
                if not file_:
                    print(
                        f"a file: {file_} to be watched doesn't exist on the filesystem"
                    )
                    continue
                if isdir:
                    print(
                        f"a file {file_} to be watched is a directory - use a watcher with `root_dir` or `glob` to watch directories"
                    )
                    continue
                else:
                    cs = await xxsum(file_)
                    # file_ is a posixpath - convert to string
                    self.checksums.put_nowait((str(file_), cs))
                    #print(
                     #   f"put {file_} in queue -getcurrent - {self.checksums.qsize()}"
                    #)
        else:
            # run the loop for file globs or root dir
            ptn = self.glob if self.glob else "*"
            async for f in aiopath.AsyncPath(self.root_dir).rglob(ptn):
                file_, isdir = await checkfile(f)
                #print(f"getcurrent - {str(file_)}")
                if isdir:
                    # skip directories - they are recursively expanded by rglob
                    continue
                cs = await xxsum(file_)
                # file_ is a posixpath - convert to string
                self.checksums.put_nowait((str(file_), cs))
                #print(f"put {file_} in queue -getcurrent - {self.checksums.qsize()}")
            print("adding finish to queue")
            await self.checksums.put("done")
        print("completed current checksums")
    async def get_watched_files(self):
        """
        gets all existing watched files from the level db instance and checks them against
        the filesystem
        """
        if self.initialized:
            print("starting get_watched")
            if hasattr(self.db, "prefix"):
                prefix = self.db.prefix.decode("utf-8")
            if not prefix:
                raise Exception("no prefix")
                
            async for item in AsyncLevelDBIterator(
                db=self.db.snapshot(), 
                exclude=[".+initialized"], 
                prefix=prefix
            ):
                if item is not None:
                    f = item[0].decode("utf-8")[len(prefix):]
                    #print(f"getwatched - {f}")
                    file_, _ = await checkfile(f)
                    if not file_:
                        # the file was deleted/moved
                        print(f"delete message for {f}")
                        self.notifications += 1
                        await self.notify_all(type_="deleted", f=f)
                        # delete the hash from the db on close - avoiding
                        # weird read/write conditions
                        self.delete_batch.put_nowait(f)
            print("finished get watched")

    async def _main(self):
        """
        wraps the main watcher functions into a task object
        """
        
        a = await asyncio.gather(self.get_watched_files(),
        self.get_current_checksums(),
        self.verify_checksums())
        
    async def run(self):
        """run."""
        start = asyncio.create_task(self.start())
        await asyncio.wait({start})
        # run tasks
        main = asyncio.create_task(self._main())
        await asyncio.wait({main})
        print(self.checksums.qsize())
        # close'r down
        close = asyncio.create_task(self.close())
        await asyncio.wait({close})
