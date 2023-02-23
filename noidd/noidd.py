#!/usr/bin/python3

import logging
import argparse
import os
import sys
import asyncio
import aiofiles.os
import plyvel
from watchers import Watcher
from notifiers import *
from utils import leveldb_aget, float_decoder
from typing import Sequence
from yaml import load, dump

try:
    from yaml import CLoader as Loader, CDumper as Dumper
except ImportError:
    from yaml import Loader, Dumper

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c", "--config", help="configuration file (.yml)", default="/etc/noidd/config.yml"
)
args = parser.parse_args()
config = None
try:
    config = load(open(args.config).read(), Loader=Loader)

except Exception as e:
    print(
        "couldn't initialize noidd - an exception occurred while loading the config.yml"
    )
    print(str(e))
    sys.exit(1)

noidd_root = "/etc/noidd" if not config["noidd_root"] else config["noidd_root"]
if not os.path.exists(noidd_root):
    try:
        os.makedirs(noidd_root)
    except Exception as e:
        print(str(e))

leveldb_file = f"{noidd_root}/noidd.db" if not config["leveldb"] else config["leveldb"]
logfile = f"{noidd_root}/noidd.log" if not config["logfile"] else config["logfile"]


class Noidd:
    """
    convenience wrapper to run all the coroutines
    """

    def __init__(self):
        self.watchers = []
        self.db = None
        self.complete = False
        self.fs_integrity = []
        self.interactive = []

    def add_watcher(self, type_:str, w):
        self.watchers.append(w)

    async def run(self):
        return asyncio.gather(*[await w.run() for w in self.watchers])


def db_initialize():
    has_leveldb = os.path.exists(leveldb_file)
    if any([not has_leveldb, config["leveldb_recreate"]]):
        print(f"creating a new db instance at {leveldb_file}")
        create = True
    else:
        print(f"found an existing db")
        create = False

    db = plyvel.DB(leveldb_file, create_if_missing=create)
    return db


async def initialize():
    db = db_initialize()
    print("initializing")
    noidd = Noidd()
    notifiers = []
    watchers = []
    # setup notifiers
    print("initializing notifiers")
    for n in config["notifiers"]:
        if n["type"] == "twilio":
            notif = TwilioNotifier(
                twilio_account_sid=n["twilio_account_sid"],
                twilio_api_key=n["twilio_api_key"],
                twilio_api_secret=n["twilio_api_secret"],
                from_number=n["twilio_from_number"],
                recipient_numbers=n["recipients"],
                batch=n["batch"],
            )
        if n["type"] == "stdout":
            notif = StdoutNotifier(batch=n["batch"])
        notifiers.append(notif)
    # setup watchers
    print("initializing watchers")
    for w in config["watchers"]["integrity"]:
        name = w["name"]
        if w["type"] == "dir":
            pfx_db = db.prefixed_db(f"{name}_".encode("utf-8"))
            init_ts = await leveldb_aget(
                db=pfx_db, key="initialized", decoder=float_decoder
            )
            if not init_ts:
                initialized = False
            else:
                initialized = True
            watcher = Watcher(
                name=name,
                db=pfx_db,
                initialized=initialized,
                notifiers=notifiers,
                root_dir=w["root_dir"],
            )
            watchers.append(watcher)
        if w["type"] == "glob":
            pfx_db = db.prefixed_db(f"{name}_".encode("utf-8"))
            print(pfx_db)
            init_ts = await leveldb_aget(db=pfx_db, key="initialized")
            if not init_ts:
                initialized = False
            else:
                initialized = True
            watcher = Watcher(
                name=name,
                db=pfx_db,
                initialized=initialized,
                notifiers=notifiers,
                glob=w["glob"],
                root_dir=w["root_dir"],
            )
            watchers.append(watcher)
        if w["type"] == "filelist":
            pfx_db = db.prefixed_db(f"{name}_".encode("utf-8"))
            init_ts = await leveldb_aget(db=pfx_db, key="initialized")
            if not init_ts:
                initialized = False
            else:
                initialized = True
            watcher = Watcher(
                name=name,
                db=pfx_db,
                initialized=initialized,
                notifiers=notifiers,
                filelist=w["files"],
            )
            watchers.append(watcher)

    noidd.add_watchers(watchers)
    return noidd


async def main():
    noidd = await initialize()
    coros = [w.run() for w in noidd.watchers]
    await asyncio.gather(*coros)


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
    loop.close()
