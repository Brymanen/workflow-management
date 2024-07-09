class Settings:
    print_rule_result_is_true_log_msgs = True
    print_rule_result_is_false_log_msgs = True
    print_then_else_branch_execution_log_msgs = True
    print_miscellaneous_log_msgs = True


class RuleResultIsTrueLogMsgs:
    @staticmethod
    def keyword_in(queried_message_content, rule_component):
        if Settings.print_rule_result_is_true_log_msgs:
            print(f'It was checked whether the keyword'
                  f' <<{str(rule_component["keyword_expr"]).lower()}>> '
                  f'is contained in <<{str(queried_message_content).lower()}>>'
                  f', this is the case.')

    @staticmethod
    def keyword_not_in(queried_message_content, rule_component):
        if Settings.print_rule_result_is_true_log_msgs:
            print(f'It was checked whether the keyword'
                  f' <<{str(rule_component["keyword_expr"]).lower()}>> '
                  f'is not contained in <<{str(queried_message_content).lower()}>>'
                  f', this is the case.')

    @staticmethod
    def number_equal(queried_message_content, rule_component):
        if Settings.print_rule_result_is_true_log_msgs:
            print(f'It was checked whether the number'
                  f' <<{rule_component["number_expr"]}>> '
                  f'is equal to'
                  f' <<{int(queried_message_content)}>>'
                  f', this is the case.')

    @staticmethod
    def number_smaller(queried_message_content, rule_component):
        if Settings.print_rule_result_is_true_log_msgs:
            print(f'It was checked whether the number'
                  f' <<{rule_component["number_expr"]}>> '
                  f'is smaller than'
                  f' <<{int(queried_message_content)}>>'
                  f', this is the case.')

    @staticmethod
    def number_smaller_or_equal(queried_message_content, rule_component):
        if Settings.print_rule_result_is_true_log_msgs:
            print(f'It was checked whether the number'
                  f' <<{rule_component["number_expr"]}>> '
                  f'is smaller or equal to'
                  f' <<{int(queried_message_content)}>>'
                  f', this is the case.')

    @staticmethod
    def numer_is_greater(queried_message_content, rule_component):
        if Settings.print_rule_result_is_true_log_msgs:
            print(f'It was checked whether the number'
                  f' <<{rule_component["number_expr"]}>> '
                  f'is greater than'
                  f' <<{int(queried_message_content)}>>'
                  f', this is the case.')

    @staticmethod
    def number_greater_or_equal(queried_message_content, rule_component):
        if Settings.print_rule_result_is_true_log_msgs:
            print(f'It was checked whether the number'
                  f' <<{rule_component["number_expr"]}>> '
                  f'is greater or equal to'
                  f' <<{int(queried_message_content)}>>'
                  f', this is the case.')


class RuleResultIsFalseLogMsgs:
    @staticmethod
    def keyword_in(queried_message_content, rule_component):
        if Settings.print_rule_result_is_false_log_msgs:
            print(f'It was checked whether the keyword'
                  f' <<{str(rule_component["keyword_expr"]).lower()}>> '
                  f'is contained in <<{str(queried_message_content).lower()}>>'
                  f', but this is not the case.')

    @staticmethod
    def keyword_not_in(queried_message_content, rule_component):
        if Settings.print_rule_result_is_false_log_msgs:
            print(f'It was checked whether the keyword'
                  f' <<{str(rule_component["keyword_expr"]).lower()}>> '
                  f'is not contained in <<{str(queried_message_content).lower()}>>'
                  f', but this is not the case.')
    
    @staticmethod
    def number_equal(queried_message_content, rule_component):
        if Settings.print_rule_result_is_false_log_msgs:
            print(f'It was checked whether the number'
                  f' <<{rule_component["number_expr"]}>> '
                  f'is equal to'
                  f' <<{int(queried_message_content)}>>'
                  f', but this is not the case.')

    @staticmethod
    def number_smaller(queried_message_content, rule_component):
        if Settings.print_rule_result_is_false_log_msgs:
            print(f'It was checked whether the number'
                  f' <<{rule_component["number_expr"]}>> '
                  f'is smaller than'
                  f' <<{int(queried_message_content)}>>'
                  f', but this is not the case.')
    
    @staticmethod
    def number_smaller_or_equal(queried_message_content, rule_component):
        if Settings.print_rule_result_is_false_log_msgs:
            print(f'It was checked whether the number'
                  f' <<{rule_component["number_expr"]}>> '
                  f'is smaller or equal to'
                  f' <<{int(queried_message_content)}>>'
                  f', but this is not the case.')
    
    @staticmethod
    def number_greater(queried_message_content, rule_component):
        if Settings.print_rule_result_is_false_log_msgs:
            print(f'It was checked whether the number'
                  f' <<{rule_component["number_expr"]}>> '
                  f'is greater than'
                  f' <<{int(queried_message_content)}>>'
                  f', but this is not the case.')

    @staticmethod
    def number_greater_or_equal(queried_message_content, rule_component):
        if Settings.print_rule_result_is_false_log_msgs:
            print(f'It was checked whether the number'
                  f' <<{rule_component["number_expr"]}>> '
                  f'is greater or equal to'
                  f' <<{int(queried_message_content)}>>'
                  f', but this is not the case.')


class ThenElseBranchExecutionLogMsgs:
    @staticmethod
    def execute_then_branch():
        if Settings.print_then_else_branch_execution_log_msgs:
            print("Executing then branch.")
    
    @staticmethod 
    def execute_else_branch():
        if Settings.print_then_else_branch_execution_log_msgs:
            print("Executing else branch.")


class MiscellaneousLogMsgs:
    @staticmethod
    def msg_published(target_queue):
        if Settings.print_miscellaneous_log_msgs:
            print(f"Message published to <<{target_queue}>> target queue.")

    @staticmethod
    def no_rules_apply():
        if Settings.print_miscellaneous_log_msgs:
            print("No rules apply, message discarded.")

    @staticmethod
    def query_location_exists_false(body, rule_component):
        if Settings.print_miscellaneous_log_msgs:
            print(f"The query location <<{rule_component['query_location']}>> "
                  f"does not exist within the message <<{body}>>.")

    @staticmethod
    def target_queue_exists_true():
        if Settings.print_miscellaneous_log_msgs:
            print("Target queue exists.")

    @staticmethod
    def target_queue_exists_false():
        if Settings.print_miscellaneous_log_msgs:
            print("Target queue does not exist.")
