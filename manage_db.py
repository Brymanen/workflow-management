import sqlite3
from datetime import datetime
import pytz


class WorkflowDB:
    """A class to manage the Workflow database."""
    def __init__(self):
        """
        Initialize the name of the db and respective tables
        and create a connection and cursor.
        """
        self.dbname = "workflow_db.sqlite"
        self.table_matched_error_msgs = "matched_error_msgs"
        self.conn = sqlite3.connect(self.dbname)
        self.cur = self.conn.cursor()

    def commit_close_conn(self):
        """Commit the changes to the db and close the connection to the db."""
        self.conn.commit()
        self.conn.close()

    def try_saving_match_to_db(self, match):
        """
        Tries to call a function to save the match to the db. If an exception
        by the db is returned then most likely the table has to be created
        first to save the match to.
        """
        try:
            self.save_match_to_db(match)
        except sqlite3.OperationalError:
            self.cur.execute(
                f'CREATE TABLE "{self.table_matched_error_msgs}" '
                f'(routing_key TEXT, article_number INTEGER, '
                f'message TEXT, date_time_added_to_db TEXT)'
            )
            self.save_match_to_db(match)

    def save_match_to_db(self, match):
        """Saves the match to the db."""
        routing_key = match['routing_key']
        article_number = int(match['article_number'])
        message = match['message']
        t = datetime.now(pytz.timezone('Europe/Berlin'))

        self.cur.execute(
            f'INSERT INTO "{self.table_matched_error_msgs}" '
            f'(routing_key, article_number, message, date_time_added_to_db) '
            f'values ("{routing_key}", "{article_number}", "{message}", "{t}")'
        )
        self.commit_close_conn()
        print('Message saved to database.')

    def query_keyword_mappings(self):
        """Queries the db for all routing key to keyword mappings."""
        query = "SELECT * FROM keyword_mapping"
        self.cur.execute(query)
        mappings = self.cur.fetchall()
        self.commit_close_conn()
        return mappings
