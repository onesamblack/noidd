#!/usr/bin/python3

"""
This is an interactive version of noidd which doesn't use file integrity watchers, but uses messages from an inotify process.
"""
import logging
import argparse
import os
import sys
import asyncio
import aiofiles.os
import plyvel
import subprocess
from notifiers import *
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

parser.add_argument(
    "-i", "--interactive", help="run in interactive mode with inotify", action="store_true", default=False
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



class NoiddInteractive:
    def __init__(self):
        self.notifiers = []
        self.inotify_running = False
        
    def add_notifier(self, notifier):
        self.notifiers.append(notifier)

    async def notify_all(self, type_: str, **kwargs):
        # print("notifying all")
        coros = []
        self.notifications += 1
        for notifier in self.notifiers:
            coros.append(notifier.notify(type_=type_, **kwargs))
        return await asyncio.gather(*coros)

    async def start_inotify(self, dir:str, events:Sequence=None):
        pass



async def initialize():
    notifiers = []
    # setup notifiers
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
