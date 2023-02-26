import xxhash
import hmac
import uuid
import asyncio
import logging
import sys
import socket
import jinja2
import time
import struct
import urllib
from functools import partial
from string import Template
from typing import Union, Sequence
from twilio.rest import Client as TwilioClient


jinja_env = jinja2.Environment(enable_async=True)
standard_message = """
        Noidd detected changes to the filesystem on host: {{ hostname }}:
        {% for m in messages %}
         - {{ m }}
        {% endfor %}
"""
message_template = jinja_env.from_string(standard_message)


class Notifier:
    """ """

    messages = {
        "modified": Template("the file: $f was modified: $t"),
        "created": Template("the file: $f was created on: $t"),
        "deleted": Template("the file: $f was deleted"),
    }

    def __init__(self, batch:bool=False, message_format: str = "py", message_limit:int=5):
        """

        Parameters
        ----------
        message_format : str
            one of 'py', 'json'. If 'py', the messages are outut as dicts.
            if 'json', the message is signed using HMAC-SHA256 and is
            output as json.
        """
        self.secret_key = xxhash.xxh64(struct.pack("f", time.time()))
        self.queue = asyncio.Queue()
        self.batch = batch
        self.message_format = message_format
        self.message_limit = message_limit
        self.watch_count = 0
        self.notifications_received = 0
        self.notifications_sent = 0
        self.template = message_template
        self.loop = asyncio.get_running_loop()
    def add_watcher(self):
        self.watch_count += 1

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
    def make_notification(cls, type_, format_, *args, **kwargs) -> Union[dict, str]:
        """
        This is an internal class method called by ```notify```

        Parameters
        ----------
        type_ :
            one of 'created', 'deleted', 'modified'.
        format_ :
            the message format, 'py' or 'json'
        kwargs :
            additional args passed by the `Watcher` class
        Returns
        -------
        Union[dict,str]

        """
        ts = time.time()
        message = cls.messages[type_].substitute(**kwargs)
        if format_ == "json":
            return json.dumps(
                {
                    "timestamp": ts,
                    "signature": self.digest(ts, message),
                    "message": message,
                }
            ).encode("utf-8")
        elif format_ == "py":
            return {"timestamp": ts, "message": message}

    async def notify(self, type_, *args, **kwargs):
        """create and send a notification. This is called by a `Watcher`

        Parameters
        ----------
        type_ :
            one of 'created', 'deleted', 'modified', 'done'
        kwargs :
            any number of format arguments. See the class variable `messages`
            for accepted arguments
        """
        if type_ == "done":
            # reduce the watch count by 1
            self.watch_count -= 1
            if self.watch_count == 0:
                # flush the remaining messages - all watchers complete
                await self.flush()
        else:
            notification = Notifier.make_notification(
                type_=type_, format_=self.message_format, *args, **kwargs
            )
            self.notifications_received += 1
            return await self.send_notification(notification, **kwargs)

    async def _get_batched_message(self) -> str:
        """gets a string representing a message batch.
        Called when the queue is full

        Returns
        -------
        str
        """

        messages_to_send = []
        while self.queue.qsize() > 0 and len(messages_to_send) <= self.message_limit:
            m = await self.queue.get()
            messages_to_send.append(m)
            self.queue.task_done()

        if messages_to_send:
            message = await self.template.render_async(
                {"hostname": socket.getfqdn(), "messages": messages_to_send}
            )
            return message
        else:
            return None
    async def send_notification(self, notification, *args, **kwargs):
        """sends messages to each recipient in self.recipients

        If batch == True, then the notifier will batch messages into a single message
        and send once the message_limit is reached

        Parameters
        ----------
        notification :
            the notification message
        args :
            additional args
        kwargs :
            additional kwargs
        """
        if self.batch:
            try:
                self.queue.put_nowait(notification["message"])
                res = None
            except asyncio.QueueFull:
                # the caller triggers the notifier to flush, then puts the
                # message in the queue once a spot is available
                res = await self.flush()
                # blocks until a spot is available
                await self.queue.put(notification["message"])
        else:
            # sending each notification as it arrives
            res = await self._send_message(notification["message"])
        self.notifications_sent += 1
        return res
    async def flush(self):
        """
        If batch == True, then the notifier will batch messages into a single message
        and send once the message_limit is reached

        Else, this is called at the end of the watch loop

        """
        message = await self._get_batched_message()
        if message is not None:
            return await self._send_message(message)
    
    async def _send_message(self, message):
        raise NotImplementedError()

class StdoutNotifier(Notifier):
    """StdoutNotifier."""

    def __init__(self, batch: bool = False, message_limit=5):
        """__init__.

        Parameters
        ----------
        batch : bool
            if True, batches notifications

        """
        super().__init__(batch=batch, message_limit=message_limit)
        self.batch = batch
        self.message_limit = message_limit
        self.message_queue = asyncio.Queue(maxsize=message_limit)


    async def _send_message(self, message: str):
        print(message)
class PushoverNotifier(Notifier):
    """PushoverNotifier."""

    jinja_env = jinja2.Environment(enable_async=True)
    message = """
        noidd detected changes to the filesystem:
        {% for m in messages %}
         - {{ m }}
        {% endfor %}
    """
    message_template = jinja_env.from_string(message)

    def __init__(
        self,
        pushover_user_key: str,
        pushover_api_token: str,
        batch: bool = False,
        message_limit: int = 15,
        live=True,
        
    ):
        """__init__.

        Parameters
        ----------
        twilio_account_sid : str
            twilio_account_sid
        twlio_api_key : str
            twlio_api_key
        from_number : str
            the twilio from number
        recipient_numbers : Sequence
            a list of recipient numbers. Each notification is sent to each number specified
        batch : bool
            if True, batches notifications

        """
        super().__init__()
        self.pushover_user_key = pushover_user_key 
        self.pushover_api_token = pushover_api_token 
        self.session = aiohttp.ClientSession("https://api.pushover.net")
        self.batch = batch
        self.message_limit = message_limit
        self.message_queue = asyncio.Queue(maxsize=message_limit)
        self.messages_send = []
        self.live = live

    async def _send_pushover_message(self, message, recipient):
        """sends a message to a single recipient via Twilio's api

        Parameters
        ----------
        message :
            the message body
        recipient :
            recipient number, formatted as `+11231234567`
        """

        def not_live(to_=None, from_=None, body=None):
            message = f"Not live: {to_}, from: {from_}, body: {body}"
            fn = partial(print, f"Not live: {to_}, from: {from_}, body: {body}")
            return fn

        if self.live:
            return not_live(to_=recipient, from_=self.from_, body=message)
        else:
            resp = await asyncio.to_thread(
                not_live(to=recipient, from_=self.from_, body=message)
            )
            return resp

    async def _send_message(self, message):
        """_send_message internal method

        Parameters
        ----------
        message :
            message
        """

        send_func = partial(self._send_pushover_message, message=message)
        coros = [send_func(recipient=x) for x in self.recipients]
        print(coros)
        responses = await asyncio.gather(*coros)
        return responses


class TwilioNotifier(Notifier):
    """TwilioNotifier."""

    jinja_env = jinja2.Environment(enable_async=True)
    message = """
        noidd detected changes to the filesystem:
        {% for m in messages %}
         - {{ m }}
        {% endfor %}
    """
    message_template = jinja_env.from_string(message)

    def __init__(
        self,
        twilio_account_sid: str,
        twilio_auth_token: str,
        from_number: str,
        recipient_numbers: Sequence,
        batch: bool = False,
        message_limit: int = 5,
        live=True,
    ):
        """__init__.

        Parameters
        ----------
        twilio_account_sid : str
            twilio_account_sid
        twlio_api_key : str
            twlio_api_key
        from_number : str
            the twilio from number
        recipient_numbers : Sequence
            a list of recipient numbers. Each notification is sent to each number specified
        batch : bool
            if True, batches notifications

        """
        super().__init__(batch=batch, message_limit=message_limit)
        self.twilio_account_sid = twilio_account_sid
        self.twilio_auth_token = twilio_auth_token
        self.from_ = from_number
        self.recipients = recipient_numbers
        self.client = TwilioClient(twilio_account_sid, twilio_auth_token)
        self.live = live


    async def _send_twilio_message(self, message, recipient):
        """sends a message to a single recipient via Twilio's api

        Parameters
        ----------
        message :
            the message body
        recipient :
            recipient number, formatted as `+11231234567`
        """

        def not_live(to_=None, from_=None, body=None):
            message = f"Not live: {to_}, from: {from_}, body: {body}"
            fn = partial(print, f"Not live: {to_}, from: {from_}, body: {body}")
            return fn

        if self.live:
            resp = await asyncio.to_thread(
                self.client.messages.create,
                to=recipient,
                from_=self.from_,
                body=message,
            )
            return resp
        else:
            resp =await asyncio.to_thread(
                not_live(to_=recipient, from_=self.from_, body=message)
            )
            return resp

    async def _send_message(self, message):
        """_send_message internal method

        Parameters
        ----------
        message :
            message
        """

        send_func = partial(self._send_twilio_message, message=message)
        coros = [send_func(recipient=x) for x in self.recipients]
        responses = await asyncio.gather(*coros)
        print(responses)
