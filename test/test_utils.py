import sys
sys.path.append("./noidd")
import asynctest
import asyncio
import random
import os
import unittest
import plyvel
import uuid
import aiofiles.os
from noidd.utils import leveldb_aput, leveldb_aget, leveldb_adelete


class TestLevelDBMethods(asynctest.TestCase):
    async def setUp(self):
        self.loop = asyncio.new_event_loop()
        testdir = "./test/test_files"
        testfile = testdir + "/test.db"
        exists = os.path.exists(testdir)
        if not exists:
            os.makedirs(testdir)
        self.db = plyvel.DB(testfile, create_if_missing=True)
        # populate
        self.kvs = []
        for i in range(0,10):
            key, value = (uuid.uuid4().hex, f"v{i}")
            self.kvs.append((key, value))
            await leveldb_aput(db=self.db, key=key, value=value)
        
    async def test_leveldb_aput(self):
        sample = self.kvs[random.randint(0,len(self.kvs))]
        await leveldb_aput(db=self.db, key=sample[0], value=sample[1])

    async def test_leveldb_aget(self):
        idx = random.randint(0,len(self.kvs))
        sample = self.kvs[idx]
        val = await leveldb_aget(db=self.db, key=sample[0])
        self.assertTrue(sample[1] == val)

    async def test_leveldb_adelete(self):
        idx = random.randint(0,len(self.kvs))
        sample = self.kvs[idx]
        val = await leveldb_aget(db=self.db, key=sample[0])
        self.assertTrue(sample[1] == val)

    async def tearDown(self):
        pass
