import pika
from pika_config import PikaConfig


def publish_msg(body, target_queue):

    # Prepare the connection to RabbitMQ.
    connection, channel = PikaConfig().get_connection_channel_exchange(
        user=PikaConfig().keyword_matching_user,
        exchange=target_queue,
    )

    # Declare a queue with the same name as the routing_key.
    # If a keyword with a new routing key is added to the keyword to routing
    # key mapping then a respective queue will be declared automatically.
    channel.queue_declare(queue=target_queue, durable=True)
    channel.queue_bind(
        queue=target_queue,
        exchange=target_queue,
        routing_key=target_queue
    )

    # Publish the msg.
    channel.basic_publish(
        exchange=target_queue,
        routing_key=target_queue,
        body=str(body),
        properties=pika.BasicProperties(
            content_type='text/plain',
            delivery_mode=2
        )
    )

    connection.close()


def publish_simulated_msg(body, target_queue):
    # Prepare the connection to RabbitMQ.
    connection, channel = PikaConfig().get_connection_channel_exchange(
        user=PikaConfig().keyword_matching_user,
        exchange=PikaConfig().root_exchange,
    )

    # Declare a queue with the same name as the routing_key.
    # If a keyword with a new routing key is added to the keyword to routing
    # key mapping then a respective queue will be declared automatically.
    channel.queue_declare(queue=target_queue, durable=True)
    channel.queue_bind(
        queue=target_queue,
        exchange=PikaConfig().root_exchange,
        routing_key=target_queue
    )

    # Publish the msg.
    channel.basic_publish(
        exchange=PikaConfig().root_exchange,
        routing_key=target_queue,
        body=str(body),
        properties=pika.BasicProperties(
            content_type='text/plain',
            delivery_mode=2
        )
    )

    connection.close()



