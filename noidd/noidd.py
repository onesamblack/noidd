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



parser = argparse.ArgumentParser()
parser.add_argument("-d","--daemon", help="run noidd as a daemon", default=True)
parser.add_argument("-c","--config", help="configuration file (.yml)", default="/etc/noidd/config.yml")
parser.add_argument("-l","--leveldb", help="filepath of leveldb", default="/etc/noidd/noidd.db")
args = parser.parse_args()

def load_config(filename:str=args.config):
    pass


def initialize_db(filename:str=args.leveldb, *args, **kwargs):
    """
    initializes a db with noidd information

    creates prefixes for all watches
    sets dates for first watch, last_watch

    """
    pass

def load_db(filename:str=args.leveldb) 
    """
    gets the noidd db or creates one if it doesn't exist
    """
    pass

def check_existing_watches() -> Sequence[str]:
    """
    check the noiddb for existing watches
    """
    pass
