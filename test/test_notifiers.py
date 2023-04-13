import sys

sys.path.append("./noidd")
import asynctest
import aiopath
import time
import asyncio
import shutil
import yaml
import random
import os
import unittest
import plyvel
from dotenv import dotenv_values
from noidd.notifiers import StdoutNotifier, TwilioNotifier
from noidd.utils import timestring

fnames = []
config = None


def setUpModule():
    # leveldb
    testdir = "test/test_files"
    exists = os.path.exists(testdir)
    if not exists:
        os.makedirs(testdir)
    for i in range(0, 100):
        fname = f"{testdir}/file{i}.txt"
        fnames.append(fname)
    config = dict(dotenv_values("test/.twilio").items())
    print(config)


class TestStdoutNotifier(asynctest.TestCase):
    async def test_stdoutnotifier_nobatch(self):
        self.notifier = StdoutNotifier(batch=False)
        self.notifier.add_watcher()
        for i in range(0, 35):
            rand = random.randint(0, 2)
            choices = ["deleted", "created", "modified"]
            choice = choices[rand]
            if i < 35:
                await self.notifier.notify(
                    type_=choice, f=fnames[i], t=timestring(time.time())
                )
            else:
                await self.notifier.notify(type_="done")

    async def test_stdoutnotifier_batch(self):
        self.notifier = StdoutNotifier(batch=True)
        self.notifier.add_watcher()
        for i in range(0, 20):
            rand = random.randint(0, 2)
            choices = ["deleted", "created", "modified"]
            choice = choices[rand]
            if i < 18:
                await self.notifier.notify(
                    type_=choice, f=fnames[i], t=timestring(time.time())
                )
            else:
                await self.notifier.notify(type_="done")


class TestTwilioNotifier(asynctest.TestCase):
    async def setUp(self):
        self.config = dict(dotenv_values("test/.twilio").items())

    async def test_twilio_notifier_nobatch(self):
        config = self.config
        self.notifier = TwilioNotifier(
            twilio_account_sid=config["TWILIO_ACCOUNT_SID"],
            twilio_auth_token=config["TWILIO_AUTH_TOKEN"],
            from_number=config["FROM_NUMBER"],
            recipient_numbers=config["RECIPIENTS"].split(","),
            batch=True,
            live=False,
        )

        self.notifier.add_watcher()
        for i in range(0, 10):
            rand = random.randint(0, 2)
            choices = ["deleted", "created", "modified"]
            choice = choices[rand]
            if i < 1:
                await self.notifier.notify(
                    type_=choice, f=fnames[i], t=timestring(time.time())
                )
            else:
                await self.notifier.notify(type_="done")

    async def test_twilio_notifier_batch(self):
        global config
        config = self.config

        self.notifier = TwilioNotifier(
            twilio_account_sid=config["TWILIO_ACCOUNT_SID"],
            twilio_auth_token=config["TWILIO_AUTH_TOKEN"],
            from_number=config["FROM_NUMBER"],
            recipient_numbers=config["RECIPIENTS"].split(","),
            batch=True,
            live=False,
        )

        self.notifier.add_watcher()
        for i in range(0, 25):
            rand = random.randint(0, 2)
            choices = ["deleted", "created", "modified"]
            choice = choices[rand]
            await self.notifier.notify(
                type_=choice, f=fnames[i], t=timestring(time.time())
            )
        await self.notifier.notify(type_="done")
