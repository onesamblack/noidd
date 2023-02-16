import pylyvel
import asyncio
from typing import Optional, Sequence, Union
from .utils  import check_leveldb, add_checksum_to_leveldb

class Watcher:
    """
    Watchers compute checksums for all the files in their watched directories.

    When they finish their process - a list of ```NoidNotifications``` is returned.
    """

    def __init__(
        self,
        glob: str,
        db: pylyvel.PrefixedDB,
        notifiers: Union[Notifier,Sequence[Notifier]],
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
        self.glob = glob
        if type(notifier) == Notifier:
            self.notifiers = [notifier]
        else:
            self.notifiers = notifiers
        self.checksums = asyncio.Queue()

    async def check_db_checksums(self):
        """Computes checksums for files in the watched directory
        
        This uses asyncio.to_thread for blocking tasks

        If a checksum didn't exist in the db, a 'created' notification is sent
        and the checksum is added to the db
        
        If a checksum didn't match, a 'modified' notification is sent

        """
        while True:
            messages = []
            f = await self.fs_files.get()
            cs = await asyncio.to_thread(check_leveldb, filename=f[0])
            if cs is None:
                # the checksum didn't exist - new file
                await asyncio.to_thread(
                    add_checksum_to_leveldb, filename=f[0], checksum=f[1]
                )
                # create a notification
                stat = await aiofiles.os.stat(str(f[0]))
                st_time = stat.st_time
                coros = []
                for notifier in self.notifiers:
                    coros.append(notifier.notify,type_="created", f=f[0], t=st_time) 
                await asyncio.gather(*coros)
            else:
                if cs != f[1]:
                    # the checksum didn't match
                    stat = await aiofiles.os.stat(str(f[0]))
                    st_time = stat.st_time
                    notif = self.notifier.notify(type_="modified", f=f[0], t=st_time)
            if f is None:
                notif = self.notifier.notify(type_="done")

        self.fs_checksums.task_done()

    async def get_fs_checksums(self):
        """get_fs_checksums.
        """
        async for f in asyncpath.rglob(self.file_glob):
            cs = await xxsum(f)
            await self.fs_checksums.put((f, cs))
        await self.fs_checksums.put(None)

    async def get_deleted_files(self):
        pass
    async def run(self):
        """run.
        """
        # first, check the filesystem to get checksums
        await self.get_fs_checksums()
        # check for any files in the db that aren't in the computed checksums
        await self.get_deleted_files()
        # check the computed checksums against the db
        await self.check_db_checksums())


