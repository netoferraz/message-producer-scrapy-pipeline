import pika
from typing import Callable, Union
import sys
import logging
from threading import Thread
from scrapy.exceptions import DropItem
import functools
import json
from pika.exceptions import AMQPError
from typing import Type

LOG_FORMAT = (
    "%(levelname) -10s %(asctime)s %(name) -30s %(funcName) "
    "-35s %(lineno) -5d: %(message)s"
)
LOGGER = logging.getLogger("pika")
LOGGER.setLevel(logging.WARNING)
LOGGER.propagate = False
channel = Type[pika.channel.Channel]


class PikaProducer:
    def __init__(
        self,
        host,
        port,
        username,
        password,
        exchange,
        exchange_type,
        queues_names,
        routing_keys,
    ):

        """RabbitMQ event producer."""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.EXCHANGE = exchange
        self.EXCHANGE_TYPE = exchange_type
        self.QUEUES = queues_names
        self.ROUTING_KEYS = routing_keys
        self._connection = None
        self._channel = None
        self._deliveries = []
        self._acked = 0
        self._nacked = 0
        self._message_number = 0
        self._stopping = False

    def open_spider(self, spider):
        try:
            self._run_pika()
        except:
            print(f"Something has wrong on trying to connect to RabbitMQ service.")
            sys.exit(1)

    def close_spider(self, spider):
        self.stop()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            host=crawler.settings.get("RABBITMQ_HOST"),
            port=crawler.settings.get("RABBITMQ_PORT"),
            username=crawler.settings.get("RABBITMQ_USERNAME"),
            password=crawler.settings.get("RABBITMQ_PASSWORD"),
            exchange=crawler.settings.get("RABBITMQ_EXCHANGE"),
            exchange_type=crawler.settings.get("RABBITMQ_EXCHANGE_TYPE"),
            queues_names=crawler.settings.get("RABBITMQ_QUEUES"),
            routing_keys=crawler.settings.get("RABBITMQ_ROUTING_KEYS"),
        )

    def _get_pika_parameters(
        self,
        connection_attempts: int = 20,
        retry_delay_in_seconds: Union[int, float] = 5,
    ) -> pika.ConnectionParameters:
        """Create Pika `Parameters`.

        Args:
            connection_attempts: number of channel attempts before giving up
            retry_delay_in_seconds: delay in seconds between channel attempts

        Returns:
            Pika `Paramaters` which can be used to create a new connection to a broker.
        """

        # host seems to be just the host, so we use our parameters
        parameters = pika.ConnectionParameters(
            host=self.host,
            port=self.port,
            credentials=pika.PlainCredentials(
                username=self.username,
                password=self.password,
            ),
            connection_attempts=connection_attempts,
            # Wait between retries since
            # it can take some time until
            # RabbitMQ comes up.
            retry_delay=retry_delay_in_seconds,
        )

        return parameters

    def _run_pika(self) -> None:
        parameters = self._get_pika_parameters()
        self._pika_connection = self.initialise_pika_select_connection(
            parameters, self._on_open_connection, self._on_open_connection_error
        )
        # Run Pika io loop in extra thread so it's not blocking
        self._run_pika_io_loop_in_thread()

    def _on_open_connection(self, connection: pika.SelectConnection) -> None:
        LOGGER.info(f"RabbitMQ connection to '{self.host}' was established.")
        connection.channel(on_open_callback=self._on_channel_open)

    def _on_open_connection_error(self, _, error: str) -> None:
        LOGGER.warning(
            f"Connecting to '{self.host}' failed with error '{error}'. Trying again."
        )

    def _on_channel_open(self, channel: pika.channel.Channel) -> None:
        LOGGER.info("RabbitMQ success channel was opened.")
        # channel.queue_declare(self.QUEUES["RABBITMQ_QUEUE_PARSER"], durable=True)
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        """This method tells pika to call the on_channel_closed method if
        RabbitMQ unexpectedly closes the channel.
        """
        LOGGER.info("Adding channel close callback")
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reason):
        """Invoked by pika when RabbitMQ unexpectedly closes the channel.
        Channels are usually closed if you attempt to do something that
        violates the protocol, such as re-declare an exchange or queue with
        different parameters. In this case, we'll close the connection
        to shutdown the object.
        :param pika.channel.Channel channel: The closed channel
        :param Exception reason: why the channel was closed
        """
        LOGGER.warning("Channel %i was closed: %s", channel, reason)
        if not self._stopping:
            # self._connection.close()
            self._channel.connection.close()
            self._channel = None

    def setup_exchange(self, exchange_name):
        """Setup the exchange on RabbitMQ by invoking the Exchange.Declare RPC
        command. When it is complete, the on_exchange_declareok method will
        be invoked by pika.
        :param str|unicode exchange_name: The name of the exchange to declare
        """
        LOGGER.info("Declaring exchange %s", exchange_name)
        # Note: using functools.partial is not required, it is demonstrating
        # how arbitrary data can be passed to the callback when it is called
        cb = functools.partial(self.on_exchange_declareok, userdata=exchange_name)
        self._channel.exchange_declare(
            exchange=exchange_name, exchange_type=self.EXCHANGE_TYPE, callback=cb
        )

    def on_exchange_declareok(self, unused_frame, userdata):
        """Invoked by pika when RabbitMQ has finished the Exchange.Declare RPC
        command.
        :param pika.Frame.Method unused_frame: Exchange.DeclareOk response frame
        """
        LOGGER.info(f"Exchange declared {userdata}.")
        for queue_name in self.QUEUES:
            self.setup_queue(queue_name)

    def setup_queue(self, queue_name):
        """Setup the queue on RabbitMQ by invoking the Queue.Declare RPC
        command. When it is complete, the on_queue_declareok method will
        be invoked by pika.
        :param str|unicode queue_name: The name of the queue to declare.
        """
        LOGGER.info("Declaring queue %s", queue_name)
        self._channel.queue_declare(queue=queue_name, callback=self.on_queue_declareok)

    def on_queue_declareok(self, _unused_frame):
        """Method invoked by pika when the Queue.Declare RPC call made in
        setup_queue has completed. In this method we will bind the queue
        and exchange together with the routing key by issuing the Queue.Bind
        RPC command. When this command is complete, the on_bindok method will
        be invoked by pika.
        :param pika.frame.Method method_frame: The Queue.DeclareOk frame
        """
        for queue_name, routing_key in zip(self.QUEUES, self.ROUTING_KEYS):
            LOGGER.info(
                "Binding %s to %s with %s", self.EXCHANGE, queue_name, routing_key
            )
            self._channel.queue_bind(
                queue_name,
                self.EXCHANGE,
                routing_key=routing_key,
                callback=self.on_bindok,
            )

    def on_bindok(self, _unused_frame):
        """This method is invoked by pika when it receives the Queue.BindOk
        response from RabbitMQ. Since we know we're now setup and bound, it's
        time to start publishing."""
        LOGGER.info("Queue bound")
        self.start_publishing()

    def start_publishing(self):
        """This method will enable delivery confirmations and schedule the
        first message to be sent to RabbitMQ
        """
        LOGGER.info("Issuing consumer related RPC commands")
        self.enable_delivery_confirmations()

    def enable_delivery_confirmations(self):
        """Send the Confirm.Select RPC method to RabbitMQ to enable delivery
        confirmations on the channel. The only way to turn this off is to close
        the channel and create a new one.
        When the message is confirmed from RabbitMQ, the
        on_delivery_confirmation method will be invoked passing in a Basic.Ack
        or Basic.Nack method from RabbitMQ that will indicate which messages it
        is confirming or rejecting.
        """
        LOGGER.info("Issuing Confirm.Select RPC command")
        self._channel.confirm_delivery(self.on_delivery_confirmation)

    def on_delivery_confirmation(self, method_frame):
        """Invoked by pika when RabbitMQ responds to a Basic.Publish RPC
        command, passing in either a Basic.Ack or Basic.Nack frame with
        the delivery tag of the message that was published. The delivery tag
        is an integer counter indicating the message number that was sent
        on the channel via Basic.Publish. Here we're just doing house keeping
        to keep track of stats and remove message numbers that we expect
        a delivery confirmation of from the list used to keep track of messages
        that are pending confirmation.
        :param pika.frame.Method method_frame: Basic.Ack or Basic.Nack frame
        """
        confirmation_type = method_frame.method.NAME.split(".")[1].lower()
        LOGGER.info(
            "Received %s for delivery tag: %i",
            confirmation_type,
            method_frame.method.delivery_tag,
        )
        if confirmation_type == "ack":
            self._acked += 1
        elif confirmation_type == "nack":
            self._nacked += 1
        self._deliveries.remove(method_frame.method.delivery_tag)
        LOGGER.info(
            "Published %i messages, %i have yet to be confirmed, "
            "%i were acked and %i were nacked",
            self._message_number,
            len(self._deliveries),
            self._acked,
            self._nacked,
        )

    def _run_pika_io_loop_in_thread(self) -> None:
        thread = Thread(target=self._run_pika_io_loop, daemon=True)
        thread.start()

    def _run_pika_io_loop(self) -> None:
        self._pika_connection.ioloop.start()

    def publish_message(self, message, routing_key):
        """If the class is not stopping, publish a message to RabbitMQ,
        appending a list of deliveries with the message number that was sent.
        This list will be used to check for delivery confirmations in the
        on_delivery_confirmations method.
        """
        if self._channel is None or not self._channel.is_open:
            return

        properties = pika.BasicProperties(
            app_id="wired-crawler", content_type="application/json"
        )

        self._channel.basic_publish(
            self.EXCHANGE,
            routing_key,
            json.dumps(message, ensure_ascii=False),
            properties,
        )
        self._message_number += 1
        self._deliveries.append(self._message_number)
        LOGGER.info("Published message # %i", self._message_number)

    def process_item(self, item, spider):
        category = item.get("category")
        if not category:
            raise DropItem("We cannot identify which category this article belongs.")
        category = category.lower()
        if category not in self.ROUTING_KEYS:
            raise DropItem(f"The category {category} is not include in our project.")
        self.publish_message(message={"url": item.get("url")}, routing_key=category)

    @staticmethod
    def close_pika_connection(connection: pika.SelectConnection) -> None:
        """Attempt to close Pika connection."""
        try:
            connection.close()
            LOGGER.debug("Successfully closed Pika connection with host.")
        except AMQPError:
            LOGGER.error("Failed to close Pika connection with host.")

    @staticmethod
    def close_pika_channel(channel: pika.channel.Channel) -> None:
        """Attempt to close Pika channel."""

        try:
            channel.close()
            LOGGER.debug("Successfully closed Pika channel.")
        except AMQPError:
            LOGGER.bind(routine="RabbitMQ").error("Failed to close Pika channel.")

    def stop(self) -> None:
        if self._channel:
            self.close_pika_channel(self._channel)
            self.close_pika_connection(self._channel.connection)

    @staticmethod
    def initialise_pika_select_connection(
        parameters: pika.ConnectionParameters,
        on_open_callback: Callable,
        on_open_error_callback: Callable,
    ) -> pika.SelectConnection:

        return pika.SelectConnection(
            parameters,
            on_open_callback=on_open_callback,
            on_open_error_callback=on_open_error_callback,
        )
