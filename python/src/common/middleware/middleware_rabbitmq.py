import time

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.spec import Basic
import random
import string
from .middleware import MessageMiddlewareQueue, MessageMiddlewareExchange

class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def _on_message_received(self, ch: BlockingChannel, method: Basic.Deliver, properties, body: bytes):
        ack_func = lambda: ch.basic_ack(delivery_tag = method.delivery_tag)
        self.on_message_callback(body, ack_func, ch.basic_nack)

    def __init__(self, host, queue_name):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host))
        self.channel = self.connection.channel()
        self.queue_name = queue_name

        self.channel.queue_declare(queue=self.queue_name, 
                                   durable=True,
                                   arguments={'x-queue-type': 'quorum'})
    
    def start_consuming(self, on_message_callback):
        self.on_message_callback = on_message_callback
        self.channel.basic_consume(queue=self.queue_name, 
                                   on_message_callback=self._on_message_received, 
                                   auto_ack=False)
        self.channel.start_consuming()
	
    def stop_consuming(self):
        self.channel.stop_consuming()
	
    def send(self, message):
        self.channel.basic_publish(exchange='',
                                   routing_key=self.queue_name,
                                   body=message)

    def close(self):
        self.connection.close()

class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):
    
    def __init__(self, host, exchange_name, routing_keys):
        pass
