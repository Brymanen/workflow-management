import sqlite3
from db_constants import DBConstants


class ReadRules:
    @staticmethod
    def establish_connection_to_db():
        # Initialize variables.
        dbname = DBConstants.db_name
        conn = sqlite3.connect(dbname)
        cur = conn.cursor()
        return cur

    @staticmethod
    def get_rules(cur):
        rule_component_table_name = DBConstants.rule_component_table_name
        cur.execute(f"SELECT * FROM {rule_component_table_name}")
        # Grab all rows from the database table
        rows = cur.fetchall()
        # Grab the column headers from said database table
        headers = [tuple[0] for tuple in cur.description]
        return rows, headers

    @staticmethod
    def get_top_level_components(rows, headers):
        # Create a list containing multiple dicts. These dicts contain the
        # values from the database table according to the database table
        # headers/column names.
        rule_component_dicts = []
        for row in rows:
            rule_component_dict = {}
            for index, header in enumerate(headers):
                rule_component_dict[header] = row[index]
            rule_component_dicts.append(rule_component_dict)

        # Determine top level component.
        top_level_components = []
        for index, rule_component_dict in enumerate(rule_component_dicts):
            if rule_component_dict[DBConstants.top_level_component_header_name] == 1:
                top_level_components.append(rule_component_dict)
        return top_level_components

    @staticmethod
    def get_unique_input_queues(top_level_components):
        """Extracts unique input queues from top level components."""
        unique_input_queues = []
        for top_level_component in top_level_components:
            if top_level_component['input_queue'] not in unique_input_queues:
                unique_input_queues.append(top_level_component['input_queue'])
        return unique_input_queues

    @staticmethod
    def get_rule_components():
        dbname = DBConstants.db_name
        rule_component_table_name = DBConstants.rule_component_table_name
        conn = sqlite3.connect(dbname)
        cur = conn.cursor()
        cur.execute(f"SELECT * FROM {rule_component_table_name}")
        rows = cur.fetchall()
        headers = [tuple[0] for tuple in cur.description]
        return rows, headers

    @staticmethod
    def get_distinct_input_queues_across_all_rule_components():
        """Extracts distinct input queues across all rule components."""
        dbname = DBConstants.db_name
        rule_component_table_name = DBConstants.rule_component_table_name
        conn = sqlite3.connect(dbname)
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT input_queue FROM {rule_component_table_name}")
        rows = cur.fetchall()
        unique_input_queues = []
        for i, row in enumerate(rows):
            if rows[i][0].lower() != "none" and rows[i][0] is not None:
                unique_input_queues.append(rows[i][0])
        return unique_input_queues

    @staticmethod
    def get_distinct_target_queues_across_all_rule_components():
        """Extracts distinct target queues across all rule components."""
        dbname = DBConstants.db_name
        rule_component_table_name = DBConstants.rule_component_table_name
        conn = sqlite3.connect(dbname)
        cur = conn.cursor()
        cur.execute(f"SELECT DISTINCT target_queue FROM {rule_component_table_name}")
        rows = cur.fetchall()
        unique_target_queues = []
        for i, row in enumerate(rows):
            if rows[i][0].lower() != "none" and rows[i][0] is not None and rows[i][0] != "Null":
                unique_target_queues.append(rows[i][0])
        return unique_target_queues


