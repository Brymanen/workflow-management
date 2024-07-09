import csv
from publish_msg import publish_simulated_msg


def read_csv(filename, max_lines):
    """Read CSV file and call actions for each line of the CSV file."""
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        header_row = next(reader)

        for index, line in enumerate(reader):
            if index <= max_lines:
                body = prepare_body(header_row, line)
                target_queue = 'error_messages'
                publish_simulated_msg(body, target_queue)


def prepare_body(header_row, line):
    """Prepare the body for the msg."""
    body = {
        header_row[0]: line[0],
        header_row[1]: line[1],
    }
    return body


read_csv(filename='./ErrorMessages.csv', max_lines=500)


