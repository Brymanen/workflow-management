import pika

from pika_config import PikaConfig
from read_rules import ReadRules
import time

def on_channel_open(channel):
    distinct_input_queues = ReadRules.get_distinct_input_queues_across_all_rule_components()
    distinct_target_queues = ReadRules.get_distinct_target_queues_across_all_rule_components()

    distinct_input_and_target_queues = distinct_input_queues
    for distinct_target_queue in distinct_target_queues:
        distinct_input_and_target_queues.append(distinct_target_queue)

    for distinct_input_and_target_queue in distinct_input_and_target_queues:
        channel.queue_delete(queue=distinct_input_and_target_queue)
        print(f'Deleted <<{distinct_input_and_target_queue}>> queue.')
    #print(connection.is_open)
    #connection.close()
    #print(connection.is_open)
    #connection.ioloop.stop()


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

