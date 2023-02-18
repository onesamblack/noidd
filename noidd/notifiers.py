from twilio.rest import Client as TwilioClient

class TwilioNotifier(Notifier):
    """TwilioNotifier.
    """
    message_template = jinja2.Template("""
        Noidd detected changes to the filesystem:
        {% for m in messages %}
         - {{ str(m) }}
        {% endfor %}
    """)

    def __init__(
        self,
        twilio_api_key: str,
        twilio_auth_sid_token: str,
        from_number: str,
        recipient_numbers: Sequence,
        batch:bool = False,
        message_limit:int = 15
    ):
        """__init__.

        Parameters
        ----------
        twilio_api_key : str
            twilio_api_key
        twilio_auth_sid_token : str
            twilio_auth_sid_token
        from_number : str
            the twilio from number
        recipient_numbers : Sequence
            a list of recipient numbers. Each notification is sent to each number specified
        batch : bool
            if True, batches notifications

        """
        super().__init__()
        self.twilio_api_key = twilio_api_key
        self.twilio_auth_sid_token = twilio_auth_sid_token
        self.from_ = from_number
        self.recipients = recipient_numbers
        self.batch = batch
        self.message_limit = message_limit
        self.message_queue = asyncio.Queue(max_size=message_limit)
        self.client = TwilioClient(twilio_api_key, twilio_auth_sid_token)

    async def _send_twilio_message(self, message, recipient):
        """sends a message to a single recipient via Twilio's api
        
        Parameters
        ----------
        message :
            the message body
        recipient :
            recipient number, formatted as `+11231234567`
        """
        await asyncio.to_thread(
            self.client.messages.create, to_=recipient, from_=self.from_, body=message
        )

    async def _send_message(message):
        """_send_message internal method

        Parameters
        ----------
        message :
            message
        """
        
        send_func = partial(self._send_twilio_message, message=message])
        coros = [send_func(recipient=x) for x in self.recipients]
        await asyncio.gather(coros)
    

    async def _get_batched_message(self) -> str:
        """gets a string representing a message batch.
        Called when the queue is full

        Returns
        -------
        str
        """

        messages_to_send = []
        if queue.empty():
            return None
        while not self.queue.empty() and len(messages_to_send) <= self.message_limit:
            m = await self.queue.get()
            messages_to_send.append(m)
            self.queue.task_done()
        
        message = TwilioNotifier.message_template.render_async(messages_to_send)
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
                self.flush()
        else:
            # sending each notification as it arrives
            await self._send_message(notification["message"])
    
    async def flush(self):
        """
        If batch == True, then the notifier will batch messages into a single message
        and send once the message_limit is reached

        Else, this is called at the end of the watch loop

        """
        message = await self.get_batched_message())
        if message is not None:
            await self._send_message(message)




class Notifier:
    """ """

    messages = {
        "modified": string.Template("the file: $f was modified: $t"),
        "created": string.Template("the file: $f was created on: $t"),
        "deleted": string.Template("the file: $f was deleted"),
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
    def make_notification(cls, type_, format_, **kwargs) -> Union[dict,str]:
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
        ts = time.now()
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

    async def notify(self, type_, **kwargs):
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
            await self.flush()
        else:
            notification = Notifier.make_notification(
                type_=type_, message=message, format_=self.format, **kwargs
            )
            await self.send_notification(notification, *args, **kwargs)

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

p
