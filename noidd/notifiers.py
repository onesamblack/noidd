import xxhash
import hmac
import uuid
import asyncio
import logging
import sys
import jinja2
import time
import struct
from functools import partial
from string import Template
from typing import Union, Sequence
from twilio.rest import Client as TwilioClient


class Notifier:
    """ """

    messages = {
        "modified": Template("the file: $f was modified: $t"),
        "created": Template("the file: $f was created on: $t"),
        "deleted": Template("the file: $f was deleted"),
    }

    def __init__(self, message_format: str = "py"):
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
        self.message_format = message_format
        self.watch_count = 0

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
            await self.send_notification(notification, **kwargs)

    async def send_notification(self, notification, *args, **kwargs):
        """
        this method is overwritten by inheriting notifiers
        """
        pass

    async def flush(self):
        """
        this method is overwritten by inheriting notifiers
        """
        pass


class StdoutNotifier(Notifier):
    """StdoutNotifier."""
    jinja_env = jinja2.Environment(enable_async=True)
    message = """
        Noidd detected changes to the filesystem:
        {% for m in messages %}
         - {{ m }}
        {% endfor %}
    """
    message_template = jinja_env.from_string(message)

    def __init__(self, batch: bool = False, message_limit: int = 15):
        """__init__.

        Parameters
        ----------
        batch : bool
            if True, batches notifications

        """
        super().__init__()
        self.batch = batch
        self.message_limit = message_limit
        self.message_queue = asyncio.Queue(maxsize=message_limit)

    async def _get_batched_message(self) -> str:
        """gets a string representing a message batch.
        Called when the queue is full

        Returns
        -------
        str
        """

        messages_to_send = []
        if self.queue.empty():
            return None
        while not self.queue.empty() and len(messages_to_send) <= self.message_limit:
            m = await self.queue.get()
            messages_to_send.append(m)
            self.queue.task_done()

        message = await StdoutNotifier.message_template.render_async({"messages": messages_to_send})
        return message

    async def _send_message(self, message: str):
        sys.stdout.write(message)

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
            if not self.queue.full():
                self.queue.put_nowait(notification["message"])
            else:
                # wait for a queue flush
                await self.flush()
                # add the message
                self.queue.put_nowait(notification["message"])

        else:
            # sending each notification as it arrives
            await self._send_message(notification["message"])

    async def flush(self):
        """
        If batch == True, then the notifier will batch messages into a single message
        and send once the message_limit is reached

        Else, this is called at the end of the watch loop

        """
        message = await self._get_batched_message()
        if message is not None:
            await self._send_message(message)

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
        twilio_auth_token:str,
        from_number: str,
        recipient_numbers: Sequence,
        batch: bool = False,
        message_limit: int = 15,
        live=True
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
        self.twilio_account_sid = twilio_account_sid
        self.twilio_auth_token = twilio_auth_token
        self.from_ = from_number
        self.recipients = recipient_numbers
        self.batch = batch
        self.message_limit = message_limit
        self.message_queue = asyncio.Queue(maxsize=message_limit)
        self.client = TwilioClient(twilio_account_sid, twilio_auth_token)
        self.messages_send = []
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
        def not_live(to_= None, from_=None, body=None):
            message = f"Not live: {to_}, from: {from_}, body: {body}"
            fn = partial(print, f"Not live: {to_}, from: {from_}, body: {body}")
            return fn
        if self.live:
            resp = await asyncio.to_thread(
                self.client.messages.create, to=recipient, from_=self.from_, body=message
            )
            return resp
        else:
            resp = await asyncio.to_thread(
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
        print(coros)
        responses = await asyncio.gather(*coros)
        return responses
    async def _get_batched_message(self) -> str:
        """gets a string representing a message batch.
        Called when the queue is full

        Returns
        -------
        str
        """

        messages_to_send = []
        if self.queue.empty():
            return None
        while not self.queue.empty() and len(messages_to_send) <= self.message_limit:
            m = await self.queue.get()
            messages_to_send.append(m)
            self.queue.task_done()

        message = await TwilioNotifier.message_template.render_async({"messages":messages_to_send})
        return message

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
            if not self.queue.full():
                self.queue.put_nowait(notification["message"])
            else:
                # wait for a queue flush
                await self.flush()
                # add the message
                self.queue.put_nowait(notification["message"])

        else:
            # sending each notification as it arrives
            await self._send_message(notification["message"])

    async def flush(self):
        """
        If batch == True, then the notifier will batch messages into a single message
        and send once the message_limit is reached

        Else, this is called at the end of the watch loop

        """
        message = await self._get_batched_message()
        if message is not None:
            await self._send_message(message)
