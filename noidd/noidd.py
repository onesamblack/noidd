import plyvel
import yaml
import xxhash
import sys
import asyncio
import argparse
import jinja2
import os
import glob
from .utils import check_leveldb, add_checksum_to_leveldb, get_db
from twilio.rest import Client as TwiClient
from multiprocessing import ThreadPool
from string import Template
from typing import Union, Sequence
import hmac
import asyncio
import json
import logging
import warnings
from functools import wraps
from typing import TypeVar, Union, AnyStr, Mapping, Iterable, Optional, AsyncGenerator

logging.basicConfig(filename=logname,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logger = logging.getLogger('noidd')

parser = argparse.ArgumentParser()
parser.add_argument("-c","--config", help="configuration file (.yml)", default="/etc/noidd/config.yml")
parser.add_argument("-l","--leveldb", help="filepath of leveldb", default="/etc/noidd/noidd.db")
parser.add_argument("--logfile", help="logging path of leveldb", default=None)
parser.add_argument("--recreate", help="recreate the leveldb", action="store_true", default=False)
args = parser.parse_args()

try:
    config = yaml.loads(args.config)

except Exception as e:
    print("couldn't initialize noidd - an exception occurred while loading the config.yml")
    print(str(e))
    sys.exit(1)


logging.basicConfig(filename=f"{config['noidd_root']}/noidd.log" if not args.logfile else args.logfile,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logging.info("Starting noidd")

logger = logging.getLogger('noidd')


class Noidd:
    def __init__(self):
        self.watchers = []
        self.db = None
        self.complete = False

async def initialize():
    logger.info("initializing")
    noidd = Noidd()
    notifiers = []
    watchers = [] 
    for w in config["watch"]:
        n = w["name"]
        if w["type"] == "dir":
            watcher = Watcher(name=n,  

    

def main(noidd):
    

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(initialize())
    loop.run_forever(main())
