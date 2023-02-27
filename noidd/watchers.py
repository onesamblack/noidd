import plyvel
import sys
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


class Watcher:
    """
    Watchers compute checksums for all the files, specified by a directory:glob pair or by a list of files
    or both!

    This recurses through all directories by default

    Be smart - don't specify child directories of a tld you can recurse through. If so, use a glob pattern

    - name: "watch_network_configs"
      directories:
        - path: "/etc/NetworkManager
          glob: "*"
        - path: /etc/security
        - glob: "*"
        - path: /etc/ca-certificates
        - glob: "*"
      files:
        - /etc/nsswitch.conf
        - /etc/nftables.conf
        - /etc/hosts
        - /etc/hosts.allow
        - /etc/hosts.deny
        - ca-certificates.conf
        - /etc/ntp.conf
        - /usr/lib/openssh

    """

    def __init__(
        self,
        name: str,
        db: plyvel.DB,
        initialized: bool,
        notifiers: Sequence[Notifier],
        filelist: Optional[Sequence] = [],
        directories: Optional[Sequence[dict]] = [],
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
        self.filelist = filelist
        self.directories = directories
        self.delete_batch = asyncio.Queue()
        try:
            self.prefix = self.db.prefix.decode("utf-8")
        except:
            self.prefix = ""
        self.checksums = asyncio.Queue()
    def __str__(self):
        return f"{self.name}, watches: {self.filelist}, {self.directories}"
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
        coros = self.notify_all(type_="done")
        await asyncio.gather(*coros)

    def notify_all(self, type_: str, **kwargs):
        coros = []
        for notifier in self.notifiers:
            coros.append(notifier.notify(type_=type_, **kwargs))
        return coros
    async def verify_checksums(self):
        print(f"starting verify {self.name}")
        while True:
            f = await self.checksums.get()
            if f != "done":
                if not self.initialized:
                    # running for the first time
                    await leveldb_aput(db=self.db, key=f[0], value=f[1])
                    self.checksums.task_done()
                else:
                    cs = await leveldb_aget(db=self.db, key=f[0])
                    if cs is None:
                        # the checksum didn't exist - new file
                        # create a notification
                        self.notifications += 1
                        stat = await aiofiles.os.stat(str(f[0]))
                        ts = stat.st_mtime
                        coros = self.notify_all(
                            type_="created", f=f[0], t=timestring(ts)
                        )
                        await asyncio.gather(*coros)
                        await leveldb_aput(db=self.db, key=f[0], value=f[1])
                        self.checksums.task_done()
                    elif cs != f[1]:
                        # the checksum didn't match
                        self.notifications += 1
                        stat = await aiofiles.os.stat(str(f[0]))
                        ts = stat.st_mtime
                        coros =  self.notify_all(
                            type_="modified", f=f[0], t=timestring(ts)
                        )
                        res = await asyncio.gather(*coros)
                        await leveldb_aput(db=self.db, key=f[0], value=f[1])
                        self.checksums.task_done()
                    else:
                        # checksum matched
                        self.checksums.task_done()
            else:
                self.checksums.task_done()
                break
    async def get_current_checksums(self):
        """get_fs_checksums."""
        if len(self.filelist) > 0:
            for f in self.filelist:
                file_, isdir = await checkfile(f)
                if not file_:
                    continue
                if isdir:
                    continue
                else:
                    try:
                        cs = await xxsum(file_)
                    except IsADirectoryError as e:
                        print(f"the file: {file_} is a directory - maybe a symlink?")
                    # file_ is a posixpath - convert to string
                    self.checksums.put_nowait((str(file_), cs))
        if len(self.directories) > 0:
            for d in self.directories:
                # run the loop for file globs or root dir
                ptn = d["glob"]
                dir_ = d["path"]
                async for f in aiopath.AsyncPath(dir_).rglob(ptn):
                    file_, isdir = await checkfile(f)
                    if not file_:
                        continue
                    if isdir:
                        continue
                    try:
                        cs = await xxsum(file_)
                    except IsADirectoryError as e:
                        print(f"the file: {file_} is a directory - maybe a symlink?")
                    # file_ is a posixpath - convert to string
                    self.checksums.put_nowait((str(file_), cs))
        await self.checksums.put("done")
    async def get_watched_files(self):
        """
        gets all existing watched files from the level db instance and checks them against
        the filesystem
        """
        print("get_watched")
        if self.initialized:
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
                    file_, _ = await checkfile(f)
                    if not file_:
                        # the file was deleted/moved
                        self.notifications += 1
                        print(f"deleted {file_}")
                        coros = self.notify_all(type_="deleted", f=f)
                        await asyncio.gather(*coros)
                        # delete the hash from the db on close - avoiding
                        # weird read/write conditions
                        self.delete_batch.put_nowait(f)

    async def _main(self):
        """
        wraps the main watcher functions into a task object
        """
        
        await self.get_watched_files()
        await asyncio.gather(self.get_current_checksums(),
                            self.verify_checksums())
        
    async def run(self):
        """run."""
        print(f"running {self.name}")
        try:
            start = asyncio.create_task(self.start())
            await asyncio.wait({start})
            # run tasks
            main = asyncio.create_task(self._main())
            await asyncio.wait({main})
            # close'r down
            close = asyncio.create_task(self.close())
            await asyncio.wait({close})
        except Exception as e:
            print(e)
            raise
