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
from dotenv import dotenv_values
from functools import partial
from aiofile import AIOFile, Writer
from noidd.notifiers import StdoutNotifier, TwilioNotifier
from noidd.utils import timestring, AsyncLevelDBIterator, checkfile
from noidd.watchers import Watcher

db = None
files = []
lock = asyncio.Lock()
fs_count = 10000
hidden_file = None
delete_files = {}
modify_files = {}

def setUpModule():
    global db, files, fs_count, hidden_file
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
        if dir != "wat":
            files.append(fname)
        dice_roll = random.random()
        if dice_roll <= delete_prob:
            delete_files[dir].append(fname)
        if not is_deleted:
            if random.random() <= modify_prob:
                modify_files[dir].append(fname)
        db.put(fname.encode("utf-8"), hash_.hexdigest().encode("utf-8"))
        if i == (fs_count -1):
            hidden_file = f"{testdir}/globby/.hidden_file"
            with open(hidden_file, "w+") as _f:
                _f.write(uuid.uuid4().hex * (random.randint(100, 1000)))
            hash_ = xxhash.xxh64()
            with open(hidden_file, mode="rb") as _f:
                chunk = _f.read(4096)
                while chunk:
                    hash_.update(chunk)
                    _f.seek(4096, 1)
                    chunk = _f.read(4096)

            files.append(hidden_file)
            db.put(hidden_file.encode("utf-8"), hash_.hexdigest().encode("utf-8"))

class TestWatcher(asynctest.TestCase):
    async def setUp(self):
        global files, delete_files, modify_files 
        
        self.config = dict(dotenv_values("test/.twilio").items())
        self.notifiers = [StdoutNotifier(batch=True), TwilioNotifier(
            twilio_account_sid=self.config["TWILIO_ACCOUNT_SID"],
            twilio_auth_token=self.config["TWILIO_AUTH_TOKEN"],
            from_number=self.config["FROM_NUMBER"],
            recipient_numbers=self.config["RECIPIENTS"].split(","),
            batch=True,
            live=True
        )]

        self.filelist = list(np.random.choice(a=files, size=50, replace=False))
        self.filelist.append(hidden_file)
        self.filelist2 = list(set(list(np.random.choice(a=files, size=50, replace=False))) - set(self.filelist))
        print(len(self.filelist))
        print(len(self.filelist2))
        self.root_dir = "test/test_files/wat"
        self.delete_files_= delete_files[os.path.basename(self.root_dir)]
        self.modify_files_ = modify_files[os.path.basename(self.root_dir)]
        self.glob = "*"
        self.assertTrue(len(self.filelist) == 51)
    
    async def modify_files(self):
        for f in self.modify_files_:
            async with AIOFile(f, "w+") as f_:
                writer = Writer(f_)
                await writer("dudeeee")
                await writer("modified these files")
                await f_.fsync()
    
    async def create_files(self, limit):
        for i in range(0, limit):
            f = f"{self.root_dir}/new_file{i}.txt"
            async with AIOFile(f, "w+") as f_:
                writer = Writer(f_)
                await writer("dudeeee")
                await writer("created these files")
                await f_.fsync()

    async def delete_files(self):
        for f in self.delete_files_:
            exists = await aiofiles.os.path.exists(f)
            if exists:
                print(f"deleting {f}")
                await aiofiles.os.remove(f)

    async def modify_files_from_list(self, _files):
        for f in _files:
            async with AIOFile(f, "w+") as f_:
                writer = Writer(f_)
                await writer("dudeeee")
                await writer("modified these files")
                await f_.fsync()
    

    async def test_rootdir_watch(self):
        global fs_count
        create_count = random.randint(0,10)
        pdb = db.prefixed_db("watched_wat_".encode("utf-8"))
        w = Watcher(
            name="watch_wat",
            db=pdb,
            initialized=False,
            notifiers=self.notifiers,
            directories=[{"path": "/test/test_files/wat", "glob": "**/*"}],
        )

        run1 = asyncio.create_task(w.run())
        await asyncio.wait({run1})
        print(run1)
        create = asyncio.create_task(
            self.create_files(limit=create_count)
        )
        await asyncio.wait({create})


        delete = asyncio.create_task(
           self.delete_files()
        )
        await asyncio.wait({delete})
        
        change = asyncio.create_task(
            self.modify_files()
        )
        await asyncio.wait({change})

        
        run2 = asyncio.create_task(w.run())
        await asyncio.wait({run2})
        print("filecount:" + str(w.fscount))
        for notifier in self.notifiers:
            self.assertEqual(notifier.notifications_received, 
                             notifier.notifications_sent)
            print(f"watchers count {notifier.watch_count}")

            print(w.notifiers[0].notifications_received)
        print("notifications sent")
        print(w.notifiers[0].notifications_sent)

        print(f"counts: {len(self.modify_files_)}, {len(self.delete_files_)}, {create_count}")
        self.assertTrue((w.notifications - (len(self.modify_files_)  + len(self.delete_files_) + create_count)) <= 5)
    async def test_pamd_watch(self):
        pdb = db.prefixed_db("watch_pamd_".encode("utf-8"))
      
        pamd_watcher = Watcher(name="watch_pamd", 
                    db=pdb, 
                    initialized=False, 
                    notifiers=self.notifiers, 
                    directories=[{"path": "/etc/pam.d", "glob": "*"}])
                
        run10 = asyncio.create_task(pamd_watcher.run())
        await asyncio.wait({run10})


        

        run11 = asyncio.create_task(pamd_watcher.run())
        await asyncio.wait({run11})
        for notifier in self.notifiers:
            self.assertEqual(notifier.notifications_received, 
                             notifier.notifications_sent)
        print(f"notifs : {pamd_watcher.notifications}")
     
    async def test_filelist_watch(self):
        pdb = db.prefixed_db("watch_frump_".encode("utf-8"))
        hidden_file = None
        for f in self.filelist:
            if ".hidden" in f:
                hidden_file = f
        filelist_watcher = Watcher(name="watch_filelist_", 
                    db=pdb, 
                    initialized=False, 
                    notifiers=self.notifiers, 
                    filelist=self.filelist)
                
        run4 = asyncio.create_task(filelist_watcher.run())
        await asyncio.wait({run4})


        change1 = asyncio.create_task(
                self.modify_files_from_list(self.filelist[-10:])
        )
        await asyncio.wait({change1})

        run5 = asyncio.create_task(filelist_watcher.run())
        await asyncio.wait({run5})
        for notifier in self.notifiers:
            self.assertEqual(notifier.notifications_received, 
                             notifier.notifications_sent)
        print(f"notifs : {filelist_watcher.notifications}")
        self.assertTrue(filelist_watcher.notifications == 10, f"{filelist_watcher.notifications}")
        self.assertIsNotNone(pdb.get(hidden_file.encode("utf-8")))
     
    async def test_filelist_and_dir_watch(self):
        pdb = db.prefixed_db("watch_filelist_dir_".encode("utf-8"))
        hidden_file = None
        for f in self.filelist2:
            if ".hidden" in f:
                hidden_file = f
        filelistdir_watcher = Watcher(name="watch_filelist_dir_", 
                    db=pdb, 
                    initialized=False, 
                    notifiers=self.notifiers, 
                    filelist=self.filelist2,
                    directories=[{"path": "/test/test_files/globby", "glob": "*/*"}])
                
        run7 = asyncio.create_task(filelistdir_watcher.run())
        await asyncio.wait({run7})


        change2 = asyncio.create_task(
                self.modify_files_from_list(self.filelist2[-10:])
        )
        await asyncio.wait({change2})

        run8 = asyncio.create_task(filelistdir_watcher.run())
        await asyncio.wait({run8})
        for notifier in self.notifiers:
            self.assertEqual(notifier.notifications_received, 
                             notifier.notifications_sent)
        print(f"notifs : {filelistdir_watcher.notifications}")
        self.assertTrue(filelistdir_watcher.notifications == 10)
     
    async def test_filelist_watch(self):
        pdb = db.prefixed_db("watch_frump_".encode("utf-8"))
        hidden_file = None
        for f in self.filelist:
            if ".hidden" in f:
                hidden_file = f
        filelist_watcher = Watcher(name="watch_filelist_", 
                    db=pdb, 
                    initialized=False, 
                    notifiers=self.notifiers, 
                    filelist=self.filelist)
                
        run4 = asyncio.create_task(filelist_watcher.run())
        await asyncio.wait({run4})


        change1 = asyncio.create_task(
                self.modify_files_from_list(self.filelist[-10:])
        )
        await asyncio.wait({change1})

        run5 = asyncio.create_task(filelist_watcher.run())
        await asyncio.wait({run5})
        for notifier in self.notifiers:
            self.assertEqual(notifier.notifications_received, 
                             notifier.notifications_sent)
        print(f"notifs : {filelist_watcher.notifications}")
        self.assertTrue(filelist_watcher.notifications == 10, f"{filelist_watcher.notifications}")
        self.assertIsNotNone(pdb.get(hidden_file.encode("utf-8")))
        
