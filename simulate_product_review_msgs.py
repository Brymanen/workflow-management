import csv
from publish_msg import publish_simulated_msg


def read_csv(filename, max_lines):
    """Read CSV file and call actions for each line of the CSV file."""
    with open(filename, 'r') as f:
        reader = csv.reader(f)
        header_row = next(reader)
        for index, line in enumerate(reader):
            if index <= max_lines:
                payload = prepare_payload(header_row, line)
                target_queue = 'product_reviews'
                publish_simulated_msg(payload, target_queue)


def prepare_payload(header_row, line):
    """Prepare the payload for the msg."""
    payload = {
        header_row[0]: line[0],
        header_row[1]: line[1],
        header_row[2]: line[2],
        header_row[3]: line[3],
        header_row[4]: line[4],
    }
    return payload

# Call read_csv and configure parameters.
read_csv(filename='./Produktbewertungen.CSV', max_lines=500)

