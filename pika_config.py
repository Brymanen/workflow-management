import pika


class PikaConfig:
    def __init__(self):
        """Initialize variables."""
        # List of all users
        self.keyword_matching_user = \
            'keyword_matching:8043utr983z4th34rgfuh34rg7'

        # All exchange URLs and their respective names
        self.keyword_matching_exchange = 'keyword_matching'
        self.keywords_matched_exchange = 'keywords_matched'

        # All queue URLs and their respective names
        self.keyword_matching_queue = 'keyword_matching'

        self.root_exchange = 'workflow_api'
        self.print_logs = True

    @staticmethod
    def get_connection_channel_exchange(user, exchange):
        """
        Creates a channel and a connection for the received username and
        exchange.
        """
        parameters = pika.URLParameters(
            f'amqp://{user}@mq-workflow.relaxdays.cloud'
            f'/#/exchanges/%2F/{exchange}'
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.exchange_declare(exchange=exchange, durable=True)
        return connection, channel

    @staticmethod
    def get_connection_channel_queue(user, queue):
        """
        Creates a channel and a connection for the received username and queue.
        """
        parameters = pika.URLParameters(
            f'amqp://{user}@mq-workflow.relaxdays.cloud'
            f'/#/queues/%2F/{queue}'
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=queue, durable=True)
        return connection, channel

