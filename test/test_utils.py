import sys
sys.path.append("./noidd")
import asynctest
import aiopath
import asyncio
import shutil
import random
import os
import unittest
import plyvel
import uuid
import xxhash
import aiofiles.os
from noidd.utils import xxsum, leveldb_aput, leveldb_aget, leveldb_adelete, float_encoder, float_decoder


db = None
kvs = []
lock = asyncio.Lock()

def setUpModule():
    global db, kvs
    # leveldb
    testdir = "test/test_files"
    testdbfile = testdir + "/test.db"
    exists = os.path.exists(testdir)
    if not exists:
        os.makedirs(testdir)
    db = plyvel.DB(testdbfile, create_if_missing=True)
    # populate
    for i in range(0,1000):
        key, value = (uuid.uuid4().hex, f"v{i}")
        kvs.append((key, value))
        db.put(key=key.encode("utf-8"), value=value.encode("utf-8"))
     # hashable files
    for i in range(0,100):
        fname = f"{testdir}/file{i}.txt"
        with open(fname, "w+") as _f:
            _f.write(uuid.uuid4().hex * (100))
        hash_ = xxhash.xxh64()
        with open(fname, mode='r') as _f:
            chunk = _f.read(4096)
            while chunk:
                hash_.update(chunk)
                _f.seek(4096,0)
                chunk = _f.read(4096)
        print(fname, hash_.hexdigest())
        db.put(fname.encode("utf-8"), hash_.hexdigest().encode("utf-8"))

def tearDownModule():
    shutil.rmtree("./test/test_files")


class TestEncoders(unittest.TestCase):
    def test_float_encoder(self):
        float_val = (0.828381201)
        encoded = float_encoder(float_val)
        self.assertTrue(type(encoded) == bytes)

    def test_float_decoder(self):
        float_val = (0.22019381201)
        encoded = float_encoder(float_val)
        decoded = float_decoder(encoded)
        self.assertAlmostEqual(decoded,float_val)



class TestLevelDBPut(asynctest.TestCase):
    async def test_leveldb_aput(self):
        global lock, db, kvs
        sample = kvs[random.randint(0,len(kvs))]
        await leveldb_aput(db=db, lock=lock, key=sample[0], value=sample[1])
    
    async def test_leveldb_aput_float(self):
        global lock, db, kvs
        value = float(12.213282881)
        idx = random.randint(0,len(kvs))
        sample = kvs[idx]
        val = await leveldb_aput(db=db, lock=lock, key=sample[0], value=value, encoder=float_encoder)
    

class TestLevelDBGet(asynctest.TestCase):
    async def test_leveldb_aget(self):
        global lock, db, kvs
        idx = random.randint(0,len(kvs))
        sample = kvs[idx]
        val = await leveldb_aget(db=db, lock=lock, key=sample[0])
        self.assertTrue(sample[1] == val)

    async def test_leveldb_aget_float(self):
        global lock, db, kvs
        value = float(.2193137231)
        idx = random.randint(0,len(kvs))
        sample = kvs[idx]
        await leveldb_aput(db=db, lock=lock, key=sample[0], value=value, encoder=float_encoder)
        val = await leveldb_aget(db=db, lock=lock, key=sample[0], decoder=float_decoder)
        self.assertAlmostEqual(value, val)
       
class TestLevelDBDelete(asynctest.TestCase):
    async def test_leveldb_adelete(self):
        idx = random.randint(0,len(kvs) - 10)
        sample = kvs[idx]
        await leveldb_adelete(db=db, lock=lock, key=sample[0])
        val = await leveldb_aget(db=db, lock=lock, key=sample[0])
        self.assertIsNone(val)

class TestXXHash(asynctest.TestCase):
    async def test_xxhash(self):
        async for f in aiopath.AsyncPath("test/test_files").glob("file*.txt"):
            hash_ = await xxsum(f)
            print(str(f), hash_)
            dbhash_ = await leveldb_aget(db=db, lock=lock, key=str(f))
            print(dbhash_)
            self.assertEqual(hash_, dbhash_)


