import sys

sys.path.append("./noidd")
import asynctest
import aiopath
import time
import aiofiles
import asyncio
import shutil
import xxhash
import yaml
import uuid
import random
import os
import numpy as np
import unittest
import plyvel
from functools import partial
from aiofile import AIOFile, Writer
from noidd.notifiers import StdoutNotifier, TwilioNotifier
from noidd.utils import timestring, AsyncLevelDBIterator, checkfile
from noidd.watchers import Watcher

db = None
files = []
lock = asyncio.Lock()
fs_count = 1000
delete_files = {}
modify_files = {}
def setUpModule():
    global db, files, fs_count
    # leveldb
    testdir = "test/test_files"
    testdbfile = testdir + "/testt.db"
    testdirs = ["globby", "listy", "dirrey", "wat"]
    for t in testdirs:
        delete_files[t] = []
        modify_files[t] = []
    exists = os.path.exists(testdir)
    testdirs_make = list(set(testdirs) | set(["wat/dude"]))
    for d in testdirs_make:
        if not os.path.exists(f"{testdir}/{d}"):
            os.makedirs(f"{testdir}/{d}")
    waitcount = 0
    db = plyvel.DB(testdbfile, create_if_missing=True)
    # populate
    # hashable files
    delete_prob = 0.01
    modify_prob = 0.1
    for i in range(0, fs_count):
        is_deleted = False
        is_modified = False
        dir = np.random.choice(testdirs, size=1)[0]
        if dir == "wat":
            dirchoices = ["wat", "wat/dude"]
            choice = np.random.choice(a=dirchoices, size=1, p=[0.3, 0.7])
            fname = f"{testdir}/{choice[0]}/file{i}.txt"
        else:
            fname = f"{testdir}/{dir}/file{i}.txt"
        with open(fname, "w+") as _f:
            _f.write(uuid.uuid4().hex * (random.randint(100, 1000)))
        hash_ = xxhash.xxh64()
        with open(fname, mode="rb") as _f:
            chunk = _f.read(4096)
            while chunk:
                hash_.update(chunk)
                _f.seek(4096, 1)
                chunk = _f.read(4096)
        files.append(fname)
        dice_roll = random.random()
        if dice_roll <= delete_prob:
            delete_files[dir].append(fname)
        if not is_deleted:
            if random.random() <= modify_prob:
                modify_files[dir].append(fname)
        db.put(fname.encode("utf-8"), hash_.hexdigest().encode("utf-8"))
    print(len(delete_files))
    print(len(modify_files))
class TestWatcher(asynctest.TestCase):
    async def setUp(self):
        global files, delete_files, modify_files 
        self.notifier = StdoutNotifier(batch=True)
        self.filelist = np.random.choice(a=files, size=50, replace=False)
        self.root_dir = "test/test_files/wat"
        self.delete_files_= delete_files[os.path.basename(self.root_dir)]
        self.modify_files_ = modify_files[os.path.basename(self.root_dir)]
        self.glob = "*"
        self.assertTrue(len(self.filelist) == 50)
        print(self.delete_files_)
        print(self.modify_files_)
    async def modify_files(self):
        print("starting modify")
        for f in self.modify_files_:
            async with AIOFile(f, "w+") as f_:
                writer = Writer(f_)
                await writer("dudeeee")
                await writer("modified these files")
                await f_.fsync()
        print("finished modify")
    
    async def create_files(self, limit):
        print("starting create")
        for i in range(0, limit):
            f = f"{self.root_dir}/new_file{i}.txt"
            async with AIOFile(f, "w+") as f_:
                writer = Writer(f_)
                await writer("dudeeee")
                await writer("created these files")
                await f_.fsync()
            print(f"created {f}")
        print("finished create")

    async def delete_files(self):
        print("starting delete")
        for f in self.delete_files_:
            exists = await aiofiles.os.path.exists(f)
            if exists:
                print(f"deleting {f}")
                await aiofiles.os.remove(f)
        print("finished delete")


    async def test_rootdir_watch(self):
        global fs_count
        create_count = random.randint(0,10)
        pdb = db.prefixed_db("watched_wat_".encode("utf-8"))
        w = Watcher(
            name="watch_wat",
            db=pdb,
            initialized=False,
            notifiers=[self.notifier],
            root_dir=self.root_dir,
        )

        run1 = asyncio.create_task(w.run())
        await asyncio.wait({run1})
        
        delete = asyncio.create_task(
           self.delete_files()
        )
        await asyncio.wait({delete})
        
        change = asyncio.create_task(
            self.modify_files()
        )
        await asyncio.wait({change})

        create = asyncio.create_task(
            self.create_files(limit=create_count)
        )
        await asyncio.wait({create})


        run2 = asyncio.create_task(w.run())
        await asyncio.wait({run2})
        print("filecount:" + str(w.fscount))
        self.assertEqual(w.notifications, len(self.modify_files_)  + \
                                          len(self.delete_files_) + \
                                          create_count)
    """
    async def test_filelist_watch(self):
        self.tasks = set()
        pdb = db.prefixed_db("watch_filelist_".encode("utf-8"))
        w = Watcher(name="watch_filelist_", 
                    db=pdb, 
                    initialized=False, 
                    notifiers=[self.notifier], 
                    root_dir=self.root_dir)
        
        task1 = await asyncio.create_task(w.run())
        task2 = await asyncio.create_task(self.modify_files(db=db,prefix="watch_filelist_",limit=2, include=[".+file.+2.+"]))
        task3 = await asyncio.create_task(w.run())
        
        
        self.assertEqual(w.notifications, 2)
    """
