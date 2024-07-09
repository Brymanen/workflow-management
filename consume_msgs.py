import pika

from manage_msg import manage_msg
from pika_config import PikaConfig
from read_rules import ReadRules


def callback(channel, method, properties, body):
    print(" [x] Received %r" % body)
    # Convert body from byte to string and then forward it along with the
    # method.
    routing_key = method.routing_key
    manage_msg(body.decode("utf-8"), routing_key)


def on_channel_open(channel):
    """Create a channel for each input queue determined in the rules."""
    cur = ReadRules.establish_connection_to_db()
    rows, headers = ReadRules.get_rules(cur)
    top_level_components = ReadRules.get_top_level_components(rows, headers)
    unique_input_queues = ReadRules.get_unique_input_queues(top_level_components)

    for unique_input_queue in unique_input_queues:
        print(f'Opening channel to {unique_input_queue}.')
        channel.basic_consume(unique_input_queue, callback, auto_ack=True)


def on_open(connection):
    print("Opening connection...")
    connection.channel(on_open_callback=on_channel_open)


user = PikaConfig().keyword_matching_user
parameters = pika.URLParameters(
        f'amqp://{user}@mq-workflow.relaxdays.cloud/#/queues/%2F/'
)

connection = pika.SelectConnection(
    parameters=parameters,
    on_open_callback=on_open
)


try:
    connection.ioloop.start()
except KeyboardInterrupt:
    print("Connection closed.")
    connection.close()
