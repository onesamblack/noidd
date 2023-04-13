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
from noidd.utils import (
    xxsum,
    leveldb_aput,
    leveldb_aget,
    leveldb_adelete,
    float_encoder,
    float_decoder,
    AsyncLevelDBIterator,
    checkfile
    )


db = None
fdb = None
kvs = []
create_count = 1000
lock = asyncio.Lock()
testsubdir = "test_prefix"
pdb = None

def setUpModule():
    global db, kvs, fdb, pdb
    # leveldb
    testdir = "test/test_files"
    testdbfile = f"{testdir}/test.db"
    fdb_file = f"{testdir}/filedb.db"
    print("starting module")
    exists = os.path.exists(testdir)
    if not exists:
        os.makedirs(testdir)
    if not os.path.exists(f"{testdir}/{testsubdir}"):
        os.makedirs(f"{testdir}/{testsubdir}")
    db = plyvel.DB(testdbfile, create_if_missing=True)
    fdb = plyvel.DB(fdb_file, create_if_missing=True)
    pdb = fdb.prefixed_db(f"{testsubdir}_".encode("utf-8"))
    print("filedb is" + str(fdb))
    # populate
    for i in range(0, 1000):
        key, value = (uuid.uuid4().hex, f"v{i}")
        kvs.append((key, value))
        db.put(key=key.encode("utf-8"), value=value.encode("utf-8"))
    # hashable files
    for i in range(0, create_count):
        usepdb = False
        if i in [x for x in range(400, 500)]:
            fname = f"{testdir}/{testsubdir}/file{i}.txt"
            usepdb = True
        else:
            fname = f"{testdir}/file{i}.txt"
        with open(fname, "w+") as _f:
            _f.write(uuid.uuid4().hex * (100))
        hash_ = xxhash.xxh64()
        with open(fname, mode="r") as _f:
            chunk = _f.read(4096)
            while chunk:
                hash_.update(chunk)
                _f.seek(4096, 0)
                chunk = _f.read(4096)
        if usepdb:
            pdb.put(fname.encode("utf-8"), hash_.hexdigest().encode("utf-8"))

        else:
            fdb.put(fname.encode("utf-8"), hash_.hexdigest().encode("utf-8"))

class TestAsyncPath(asynctest.TestCase):
    async def test_asyncpath(self):
        count = 0
        async for f in aiopath.AsyncPath("test/test_files").rglob("file*"):
            file_, isdir = await checkfile(str(f))
            if not isdir:
                count +=1
        self.assertEqual(count,1000)
   
class TestEncoders(unittest.TestCase):
    def test_float_encoder(self):
        float_val = 0.828381201
        encoded = float_encoder(float_val)
        self.assertTrue(type(encoded) == bytes)

    def test_float_decoder(self):
        float_val = 0.22019381201
        encoded = float_encoder(float_val)
        decoded = float_decoder(encoded)
        self.assertAlmostEqual(decoded, float_val)


class TestLevelDBPut(asynctest.TestCase):
    async def test_leveldb_aput(self):
        global lock, db, kvs
        sample = kvs[random.randint(0, len(kvs))]
        await leveldb_aput(db=db, key=sample[0], value=sample[1])

    async def test_leveldb_aput_float(self):
        global lock, db, kvs
        value = float(12.213282881)
        idx = random.randint(0, len(kvs))
        sample = kvs[idx]
        val = await leveldb_aput(
            db=db, key=sample[0], value=value, encoder=float_encoder
        )


class TestLevelDBGet(asynctest.TestCase):
    async def test_leveldb_aget(self):
        global lock, db, kvs
        idx = random.randint(0, len(kvs))
        sample = kvs[idx]
        val = await leveldb_aget(db=db, key=sample[0])
        self.assertTrue(sample[1] == val)

    async def test_leveldb_aget_float(self):
        global lock, db, kvs
        value = float(0.2193137231)
        idx = random.randint(0, len(kvs))
        sample = kvs[idx]
        await leveldb_aput(db=db, key=sample[0], value=value, encoder=float_encoder)
        val = await leveldb_aget(db=db, key=sample[0], decoder=float_decoder)
        self.assertAlmostEqual(value, val)


class TestLevelDBDelete(asynctest.TestCase):
    async def test_leveldb_adelete(self):
        idx = random.randint(0, len(kvs) - 10)
        sample = kvs[idx]
        await leveldb_adelete(db=db, key=sample[0])
        val = await leveldb_aget(db=db, key=sample[0])
        self.assertIsNone(val)


class TestXXHash(asynctest.TestCase):
    async def test_xxhash(self):
        async for f in aiopath.AsyncPath("test/test_files").glob("file*.txt"):
            hash_ = await xxsum(f)
            dbhash_ = await leveldb_aget(db=fdb, key=str(f))
            self.assertEqual(hash_, dbhash_)

    async def test_xxhash_pdb(self):
        async for f in aiopath.AsyncPath("test/test_files/test_prefix").glob(
            "file*.txt"
        ):
            hash_ = await xxsum(f)
            dbhash_ = await leveldb_aget(db=pdb, key=str(f))
            self.assertEqual(hash_, dbhash_)


class TestLevelDBAsyncIterator_file(asynctest.TestCase):
    async def setUp(self):
        self.db = fdb

    async def test_iterator(self):
        count = 0
        async for f in AsyncLevelDBIterator(db=self.db):
            if f:
                count += 1
        self.assertEqual(count, create_count)

    async def test_iterator_include(self):
        files = []
        async for f in AsyncLevelDBIterator(db=self.db, include=[".+file6.+\.txt"]):
            if f:
                files.append(f[0])
        self.assertEqual(len(files), 110)

    async def test_iterator_exclude(self):
        files = []
        async for f in AsyncLevelDBIterator(db=self.db, exclude=[".+file9.+\.txt"]):
            if f:
                files.append(f[0])
        self.assertEqual(len(files), 890)

class TestLevelDBAsyncIterator_prefix_snapshot(asynctest.TestCase):
    async def setUp(self):
        self.db = pdb.snapshot()
        self.prefix = pdb.prefix.decode("utf-8")

    async def test_iterator_pdb(self):
        files = []
        async for f in AsyncLevelDBIterator(db=self.db, prefix=self.prefix):
            if f:
                files.append(f[0])
        self.assertEqual(len(files), 100)

    async def test_iterator_pdb_include(self):
        files = []
        async for f in AsyncLevelDBIterator(
            db=self.db, include=[".+file45[0-9]{1}\.txt"],
            prefix=self.prefix
            ):
            if f:
                files.append(f[0])
        self.assertEqual(len(files), 10)

    async def test_iterator_pdb_exclude(self):
        files = []
        async for f in AsyncLevelDBIterator(
            db=self.db, exclude=[".+file42[0-9]\.txt"],
            prefix=self.prefix
            ):
            if f:
                files.append(f[0])
        self.assertEqual(len(files), 90)
class TestLevelDBAsyncIterator_prefix(asynctest.TestCase):
    async def setUp(self):
        self.db = pdb

    async def test_iterator_pdb(self):
        files = []
        async for f in AsyncLevelDBIterator(db=self.db):
            if f:
                files.append(f[0])
        self.assertEqual(len(files), 100)

    async def test_iterator_pdb_include(self):
        files = []
        async for f in AsyncLevelDBIterator(
            db=self.db, include=[".+file45[0-9]{1}\.txt"]
        ):
            if f:
                files.append(f[0])
        self.assertEqual(len(files), 10)

    async def test_iterator_pdb_exclude(self):
        files = []
        async for f in AsyncLevelDBIterator(
            db=self.db, exclude=[".+file42[0-9]\.txt"]
        ):
            if f:
                files.append(f[0])
        self.assertEqual(len(files), 90)
