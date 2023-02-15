import plyvel
import yaml
import socket
import xxhash
import sys
import asyncio
import argparse
import os
import glob
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




class Notifier:
    """

    """
    messages = {
            "modified": Template("the file: $f was modified: $t"),
            "created" : Template("the file: $f was created on: $t"),
            "deleted" : Template("the file: $f was deleted")
    }

    def __init__(self, message_format:str="py"):
        self.secret_key = xxhash.xxh64(time.time().encode("utf-8")) 
        self.queue = asyncio.Queue()
        self.message_format = message_format
    
    def digest(self, timestamp, message):
        """
        reserved for future use

        digest = hmac.new(self.secret_key, 
                          f"{timestamp}/{message}".encode("utf-8"), 
                          digestmod=hashlib.sha256).hexdigest()
        return digest
        """
        pass
    
    @classmethod
    def make_notification(cls, type_, message, format_, **kwargs):
        ts = time.now()
        message = cls.messages[type_].substitute(**kwargs)
        if format_ == "json":
            return json.dumps({"timestamp": ts, 
                                "signature": self.digest(ts, message), 
                                "message": message}).encode("utf-8")
        elif format_ == "py":
            return {"timestamp": ts, "message": message}
    
    async def notify(self, type_, message, **kwargs):
        notification = Notifier.make_notification(type_=type_, 
                                                  message=message, 
                                                  format_=self.format, 
                                                  **kwargs)
        await self.send_notification(notification, *args, **kwargs)

    async def send_notification(self, notification, *args, **kwargs):
        """
        this method is overridden by inheriting notifiers
        """
        pass


class TwilioNotifier(Notifier):
    def __init__(self, 
                 twilio_api_key:str, 
                 twilio_auth_sid_token:str, 
                 from_number:str,
                 recipient_numbers:Sequence):
        super().__init__()
        self.twilio_api_key = twilio_api_key
        self.twilio_auth_sid_token = twilio_auth_sid_token
        self.from_ = from_number
        self.recipients = recipient_numbers
        self.client = TwiClient(twilio_api_key, twilio_auth_sid_token)
    
    async def send_message(self, message, recipient):
        """
        send message sends a message to a single recipient
        """
        await asyncio.to_thread(self.client.messages.create, to_=recipient, from_=self.from_, body=message)
    
    async def send_notification(self, notification, *args, **kwargs):
        send_func = partial(self.send_message, notification["message"])
        coros = [send_func(recipient=x) for x in self.recipients]
        await asyncio.gather(coros)


async def xxsum(filename:str):
    h = xxhash.xxh64()
    async with AIOFile(filename,"rb") as f:
        reader = Reader(f, chunk_size=512)
        async for chunk in reader:
            h.update(chunk)
    return h.hexdigest()

            
class Watcher:
    """
    Watchers compute checksums for all the files in their watched directories.

    When they finish their process - a list of ```NoidNotifications``` is returned.
    """
    def __init__(self,
                 glob:str,
                 db: pylyvel.PrefixedDB,
                 notifier:Notifier,
                 loop: Optional[asyncio.AbstractEventLoop] = None,
                 ):
        self._loop = loop or asyncio.get_event_loop()
        self.glob = glob
        self.notifier = notifier
        self.writer = notification_writer 
        self.checksums = asyncio.Queue()
    
    def _check_db(self, filename:str):
        """
        runs a blocking task - checks the leveldb instance for the filename
        """
        cs = self.db.get(filename)
        return cs

    def _add_checksum(self, filename:str, checksum:str):
        """
        blocking task - adds a checksum to the db
        """
        self.db.put(str(filename).encode(), str(checksum).encode())

    async def check_db_checksums(self):
        while True:
            f = await self.fs_files.get()
            cs = await asyncio.to_thread(self._check_db, filename=f[0])
            if cs is None:
                # the checksum didn't exist - new file
                await asyncio.to_thread(self._add_checksum, filename=f[0], checksum=f[1])
                # create a notification
                stat = await aiofiles.os.stat(str(f[0]))
                st_time = stat.st_time
                notif = await self.notifier.notify(type_="created", 
                                                   f=f[0], 
                                                   t=st_time))
            else:
                if cs != f[1]:
                    # the checksum didn't match
                    stat = await aiofiles.os.stat(str(f[0]))
                    st_time = stat.st_time
                    notif = self.notifier.notify(type_="modified", 
                                                 f=f[0], 
                                                 t=st_time))
            if f is None:
                break
        self.fs_checksums.task_done()

    async def get_fs_checksums(self):
        async for f in asyncpath.rglob(self.file_glob):
            cs = await xxsum(f)
            await self.fs_checksums.put((f,cs))
        await self.fs_checksums.put(None)    
    
    async def run(self):
        await asyncio.gather(self.get_fs_checksums(), self.check_db_checksums()
        
    
   
               
def create_socket(fd:str) -> boolean:
    if 


def checksum(filename:str) -> str:
    """
    creates a checksum of the file contents using xxhash
    """
    checksum = xxhash.xx64()
    with open(filename, "rb") as f:
        data = f.seek(512)
        while data:
            checksum.update(data)
            data = f.seek(512,1)
    
    return checksum.hexdigest()


def get_db(*args, **kwargs) -> Sequence[plyvel.PrefixedDB]:
    """
    returns one or more instances of a ```plyvel.PrefixedDB```. 
    If a primary noidd db doesn't exist, it's created.

    At a minimum, noidd creates one leveldb instance, with one prefix 'main'
    to store configuration details. 

    For each watched directory an instance of ```plyvel.PrefixedDB``` is returned.

    """

    db_filename = config["db_file"]
    db = plyvel.DB(db_filename, **config["db_options"])










