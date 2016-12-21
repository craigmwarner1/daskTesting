from amqpy import Connection, Message, AbstractConsumer
import json
from os import environ as e


class Communicator:
    def __init__(self, queue, *subscription_keys):
        """ Setup a connection to the RabbitMQ server.

            Args:
                *subscription_keys (str): RabbitMQ topics worker will subscribe to.
        """
        self.channel = None
        self.connection = None
        self.exchange_name = None
        self.queue_name = queue
        self.subscriptions = subscription_keys

        self.open_channel()

    def open_channel(self):
        """Opens connection to RabbitMQ"""

        # these should all come from Docker Compose but just in case use the defaults
        host = e['RABBIT_HOST'] if 'RABBIT_HOST' in e else '10.128.0.2'
        port = e['RABBIT_PORT'] if 'RABBIT_PORT' in e else '5672'
        user = e['RABBIT_USER'] if 'RABBIT_USER' in e else 'guest'
        pswd = e['RABBIT_PASS'] if 'RABBIT_PASS' in e else 'guest'

        self.exchange_name = e['RABBIT_EXCHANGE'] if 'RABBIT_EXCHANGE' in e else 'test-exchange'
#        self.queue_name = e['RABBIT_QUEUE'] if 'RABBIT_QUEUE' in e else 'test-queue'

        # open connection
        self.connection = Connection(
            host = host,
            port = port,
            userid = user,
            password = pswd,
            heartbeat = 60
        )

        # open a channel on the connection
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count = 1)

        # connect the channel to an exchange (creates if does not exist)
        self.channel.exchange_declare(
            exchange = self.exchange_name,
            exch_type = 'topic',
            auto_delete = False
        )

        # attach to a queue on the exchange (creates if does not exist)
        self.channel.queue_declare(
            queue = self.queue_name,
            durable = True,
            auto_delete = False
        )

        # set binding keys for queue to listen for
        for subscription in self.subscriptions:
            self.channel.queue_bind(
                queue = self.queue_name,
                exchange = self.exchange_name,
                routing_key = subscription
            )

    def publish(self, message, key):
        """ Publish a message to a queue.

            Args:
                message (dict): Dictionary of key value pairs that constitute the message body.
                key (str): RabbitMQ topic used to route message.
        """

        message = json.dumps(message, ensure_ascii = False)
        self.channel.basic_publish(
            msg = Message(message),
            exchange = self.exchange_name,
            routing_key = key
        )

    def listen(self, action):
        """ Start listening for incoming messges.

            Args:
                action (function): A function to be executed when a message is received.
        """
        class Consumer(AbstractConsumer):
            def run(self, msg: Message):
                data = json.loads(msg.body)
                output = action(data)  # do this with the message
                msg.ack()

        consumer = Consumer(
            channel = self.channel,
            queue = self.queue_name
        )
        consumer.declare()
        while True:
            self.connection.drain_events(timeout = None)
