import ast

from jsonpath_ng import parse

from publish_msg import publish_msg
from db_constants import DBConstants
from log_msgs import (
    RuleResultIsTrueLogMsgs,
    RuleResultIsFalseLogMsgs,
    ThenElseBranchExecutionLogMsgs,
    MiscellaneousLogMsgs,
)
from read_rules import ReadRules


def manage_msg(body, routing_key):
    body = ast.literal_eval(body)
    rows, headers = ReadRules.get_rule_components()
    rule_component_dicts = []

    # Create a list containing multiple dicts. These dicts contain the values
    # from the database table according to the database table headers/column
    # names.
    for row in rows:
        rule_component_dict = {}
        for index, header in enumerate(headers):
            rule_component_dict[header] = row[index]
        rule_component_dicts.append(rule_component_dict)

    top_level_components = []
    # Determine top level component.
    for index, rule_component_dict in enumerate(rule_component_dicts):
        if rule_component_dict[DBConstants.top_level_component_header_name] == 1:
            top_level_components.append(rule_component_dict)

    for top_level_component in top_level_components:
        if routing_key == top_level_component['input_queue']:
            check_for_applying_rule_components(
                body=body,
                routing_key=routing_key,
                rule_component=top_level_component,
                rule_component_dicts=rule_component_dicts,
                queried_message_content=""
            )


def check_for_applying_rule_components(body, routing_key, rule_component,
                                       rule_component_dicts, queried_message_content):
    target_queue = rule_component["target_queue"]
    if check_if_query_location_exists(body, rule_component):
        queried_message_content = parse(rule_component["query_location"]).find(body)[0].value

    # Handle rule components only containing a target queue.
    if \
            rule_component["keyword_expr"] == 'None' \
                    and rule_component["operator_expr"] == 'None' \
                    and rule_component["number_expr"] is None \
                    and rule_component["then_event_expr"] == 'None' \
                    and rule_component["else_event_expr"] == 'None' \
                    and rule_component["target_queue"] != 'None':
        process_applicable_rule(body, routing_key, target_queue,
                                queried_message_content, rule_component,
                                rule_component_dicts)

    # Handle rule components containing keywords.
    elif rule_component["operator_expr"] == "in" \
            and rule_component["keyword_expr"] is not None:
        if str(rule_component["keyword_expr"]).lower() in str(queried_message_content).lower() \
                and rule_component["operator_expr"] == "in":
            RuleResultIsTrueLogMsgs.keyword_in(queried_message_content, rule_component)
            process_applicable_rule(body, routing_key, target_queue,
                                    queried_message_content, rule_component,
                                    rule_component_dicts)
        else:
            RuleResultIsFalseLogMsgs.keyword_in(queried_message_content, rule_component)
            check_else_branch(body, queried_message_content, routing_key, rule_component, rule_component_dicts,
                              target_queue)
    elif rule_component["operator_expr"] == "not in" \
            and rule_component["keyword_expr"] is not None:
        if str(rule_component["keyword_expr"]).lower() not in str(queried_message_content).lower() \
                and rule_component["operator_expr"] == "not in":
            RuleResultIsTrueLogMsgs.keyword_not_in(queried_message_content, rule_component)
            process_applicable_rule(body, routing_key, target_queue,
                                    queried_message_content, rule_component,
                                    rule_component_dicts)
        else:
            RuleResultIsFalseLogMsgs.keyword_not_in(queried_message_content, rule_component)
            check_else_branch(body, queried_message_content, routing_key, rule_component, rule_component_dicts,
                              target_queue)
    # Handle rule components containing numbers.
    elif rule_component["operator_expr"] == "==" \
            and rule_component["number_expr"] is not None:
        if rule_component["number_expr"] == int(queried_message_content):
            RuleResultIsTrueLogMsgs.number_equal(queried_message_content, rule_component)
            process_applicable_rule(body, routing_key, target_queue,
                                    queried_message_content, rule_component,
                                    rule_component_dicts)
        else:
            RuleResultIsFalseLogMsgs.number_equal(queried_message_content, rule_component)
            check_else_branch(body, queried_message_content, routing_key, rule_component, rule_component_dicts,
                              target_queue)
    elif rule_component["operator_expr"] == "<" \
            and rule_component["number_expr"] is not None:
        if rule_component["number_expr"] > int(queried_message_content):
            RuleResultIsTrueLogMsgs.number_smaller(queried_message_content, rule_component)
            process_applicable_rule(body, routing_key, target_queue,
                                    queried_message_content, rule_component,
                                    rule_component_dicts)
        else:
            RuleResultIsFalseLogMsgs.number_smaller(queried_message_content, rule_component)
            check_else_branch(body, queried_message_content, routing_key, rule_component, rule_component_dicts,
                              target_queue)
    elif rule_component["operator_expr"] == ">" \
            and rule_component["number_expr"] is not None:
        if rule_component["number_expr"] < int(queried_message_content):
            RuleResultIsTrueLogMsgs.numer_is_greater(queried_message_content, rule_component)
            process_applicable_rule(body, routing_key, target_queue,
                                    queried_message_content, rule_component,
                                    rule_component_dicts)
        else:
            RuleResultIsFalseLogMsgs.number_greater(queried_message_content, rule_component)
            check_else_branch(body, queried_message_content, routing_key, rule_component, rule_component_dicts,
                              target_queue)
    elif rule_component["operator_expr"] == ">=" \
            and rule_component["number_expr"] is not None:
        if rule_component["number_expr"] <= int(queried_message_content):
            RuleResultIsTrueLogMsgs.number_greater_or_equal(queried_message_content, rule_component)
            process_applicable_rule(body, routing_key, target_queue,
                                    queried_message_content, rule_component,
                                    rule_component_dicts)
        else:
            RuleResultIsFalseLogMsgs.number_greater_or_equal(queried_message_content, rule_component)
            check_else_branch(body, queried_message_content, routing_key, rule_component, rule_component_dicts,
                              target_queue)
    elif rule_component["operator_expr"] == "<=" \
            and rule_component["number_expr"] is not None:
        if rule_component["number_expr"] >= int(queried_message_content):
            RuleResultIsTrueLogMsgs.number_smaller_or_equal(queried_message_content, rule_component)
            process_applicable_rule(body, routing_key, target_queue,
                                    queried_message_content, rule_component,
                                    rule_component_dicts)
        else:
            RuleResultIsFalseLogMsgs.number_smaller_or_equal(queried_message_content, rule_component)
            check_else_branch(body, queried_message_content, routing_key, rule_component, rule_component_dicts,
                              target_queue)
    #else:
        #MiscellaneousLogMsgs.no_rules_apply()


def check_else_branch(body, queried_message_content, routing_key, rule_component, rule_component_dicts, target_queue):
    if downstream_rule_component_else_exists(rule_component):
        ThenElseBranchExecutionLogMsgs.execute_else_branch()
        find_downstream_rule_component_else(
            body=body, routing_key=routing_key, target_queue=target_queue,
            queried_message_content=queried_message_content,
            rule_component=rule_component,
            rule_component_dicts=rule_component_dicts
        )


def check_if_query_location_exists(body, rule_component):
    """ Check whether the query location exists within the message."""
    try:
        parse(rule_component["query_location"]).find(body)[0].value
        return True
    except Exception:
        MiscellaneousLogMsgs.query_location_exists_false(body, rule_component)
        print("The query_location does not exist within the message.")
        return False


def process_applicable_rule(body, routing_key, target_queue,
                            queried_message_content, rule_component,
                            rule_component_dicts):
    if downstream_rule_component_then_exists(rule_component) is False:
        if target_queue_exists(target_queue):
            publish_msg(body, target_queue)
            MiscellaneousLogMsgs.msg_published(target_queue)
    elif downstream_rule_component_then_exists(rule_component) is True:
        if target_queue_exists(target_queue):
            publish_msg(body, target_queue)
            MiscellaneousLogMsgs.msg_published(target_queue)
        find_downstream_rule_component_then(
            body, routing_key, target_queue, queried_message_content,
            rule_component, rule_component_dicts
        )


def target_queue_exists(target_queue):
    if target_queue != 'None' \
            and target_queue != 'NONE' \
            and target_queue != 'Null' \
            and target_queue != 'NULL' \
            and target_queue is not None:
        MiscellaneousLogMsgs.target_queue_exists_true()
        return True
    else:
        MiscellaneousLogMsgs.target_queue_exists_false()
        return False


def downstream_rule_component_then_exists(rule_component):
    if \
            (rule_component['then_event_expr'] != 'None') \
                    and (rule_component['then_event_expr'] != 'NONE') \
                    and (rule_component['then_event_expr'] != 'Null') \
                    and (rule_component['then_event_expr'] != 'NULL') \
                    and (rule_component['then_event_expr'] is not None):
        return True
    else:
        return False


def downstream_rule_component_else_exists(rule_component):
    if \
            (rule_component['else_event_expr'] != 'None') \
                    and (rule_component['else_event_expr'] != 'NONE') \
                    and (rule_component['else_event_expr'] != 'Null') \
                    and (rule_component['else_event_expr'] != 'NULL') \
                    and (rule_component['else_event_expr'] is not None):
        return True
    else:
        return False


def find_downstream_rule_component_then(body, routing_key, target_queue,
                                        queried_message_content,
                                        rule_component,
                                        rule_component_dicts):
    for rule_component_dict in rule_component_dicts:
        if int(rule_component['then_event_expr']) == int(rule_component_dict['id']):
            ThenElseBranchExecutionLogMsgs.execute_then_branch()
            check_for_applying_rule_components(
                body=body, routing_key=routing_key,
                rule_component=rule_component_dict,
                rule_component_dicts=rule_component_dicts,
                queried_message_content=queried_message_content
            )
            break


def find_downstream_rule_component_else(body, routing_key, target_queue, queried_message_content, rule_component,
                                        rule_component_dicts):
    for rule_component_dict in rule_component_dicts:
        if int(rule_component['else_event_expr']) == int(rule_component_dict['id']):
            ThenElseBranchExecutionLogMsgs.execute_else_branch()
            check_for_applying_rule_components(
                body=body, routing_key=routing_key,
                rule_component=rule_component_dict,
                rule_component_dicts=rule_component_dicts,
                queried_message_content=queried_message_content
            )
            break


#body = "{'article_number': '10034236','message': 'Code: 4000 you, Description: Price too high'}"
#manage_msg(body, 'error_messages')

#body = "{'ASIN': 'B074KL49L7', 'date': '4/17/2020', 'variant': 'Silver', 'stars': '2', 'customer_review_text': 'glas Schrauben sind die falsch. Musste selber welche besorgen. Auf den Bildern sieht der Tisch qualitativ besser aus als in Wirklichkeit.'}"
#manage_msg(body, 'product_reviews')
