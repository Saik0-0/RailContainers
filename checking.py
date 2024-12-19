import numpy as np


class CheckingRules:
    def check_rules(self, SC: dict, containersSPR_table: dict, rulesSPR_table: dict, tableSPR_table: dict):
        for sessionID, session in SC.items():
            print(sessionID)
            # session[0] - платформы для данной сессии
            # session[1] - перестановки контейнеров для данной сессии
            for platform in session[0]:
                if platform[1] in list(containersSPR_table.keys()):
                    flag_for_stop_checking = False
                    for container_part in session[1]:
                        if not flag_for_stop_checking:
                            correct_codes = []
                            if len(container_part) in containersSPR_table[platform[1]].keys():
                                for codes in containersSPR_table[platform[1]][len(container_part)]:
                                    """Добавить проверку на классы опасности"""
                                    if len(container_part) == len(codes):
                                        if (((np.array(container_part, dtype=object)[:, 2]).astype(str) == (
                                        np.array(codes, dtype=object)[:, 4]).astype(str)).all()
                                                and ((np.array(container_part, dtype=object)[:, 4]).astype(str) == (
                                                np.array(codes, dtype=object)[:, 6]).astype(str)).all()):
                                            correct_codes.append(np.array(codes, dtype=object)[0, 0])
                                container_part_np = np.array(container_part, dtype=object)
                                if platform[1] in rulesSPR_table.keys():
                                    for code in list(rulesSPR_table[platform[1]].keys()):
                                        if str(code) in correct_codes:
                                            rule_code = np.array(rulesSPR_table[platform[1]][code], dtype=object)
                                            flags = [False for i in range(len(rule_code))]
                                            for index, row in enumerate(rule_code):
                                                if row[0] == 'Масса':
                                                    for ind in row[1]:
                                                        if row[2] == 'Меньше равно':
                                                            if container_part_np[ind - 1, 3] <= row[3]:
                                                                flags[index] = True
                                                        elif row[2] == 'Больше равно':
                                                            if container_part_np[ind - 1, 3] >= row[3]:
                                                                flags[index] = True
                                                elif row[0] == 'Сумма масс':
                                                    weight_sum = 0
                                                    for ind in row[1]:
                                                        weight_sum += container_part_np[ind - 1, 3]
                                                    if row[2] == 'Меньше равно':
                                                        if weight_sum <= row[3]:
                                                            flags[index] = True
                                                    elif row[2] == 'Больше равно':
                                                        if weight_sum >= row[3]:
                                                            flags[index] = True
                                                elif row[0] == 'Разность масс':
                                                    """В разности масс только по 2 контейнера?"""
                                                    weight_diff = abs(
                                                        container_part_np[row[1][0] - 1, 3] - container_part_np[
                                                            row[1][1] - 1, 3])
                                                    if row[2] == 'Меньше равно':
                                                        if weight_diff <= row[3]:
                                                            flags[index] = True
                                                    elif row[2] == 'Больше равно':
                                                        if weight_diff >= row[3]:
                                                            flags[index] = True
                                                elif row[0] == 'Макс масса':
                                                    flag = True
                                                    for ind in row[1]:
                                                        if flag:
                                                            if container_part_np[ind - 1, 3] <= row[3]:
                                                                flags[index] = True
                                                            else:
                                                                flags[index] = False
                                                                flag = False
                                                elif row[0] == 'Мин масса':
                                                    flag = True
                                                    for ind in row[1]:
                                                        if flag:
                                                            if container_part_np[ind - 1, 3] >= row[3]:
                                                                flags[index] = True
                                                            else:
                                                                flags[index] = False
                                                                flag = False
                                            if (np.array(flags) == False).any():
                                                code = str(code)
                                                correct_codes.remove(code)
                                if platform[1] in tableSPR_table.keys():
                                    for code in list(tableSPR_table[platform[1]].keys()):
                                        if str(code) in correct_codes:
                                            table_conditions = np.array(tableSPR_table[platform[1]][code], dtype=object)
                                            flags = [False for i in range(len(table_conditions))]
                                            for index, condition in enumerate(table_conditions):
                                                if condition[0][0] == 'Масса':
                                                    if (condition[0][2] <= container_part_np[
                                                        condition[0][1][0] - 1, 3] <= condition[0][3]
                                                            and condition[1][2] <= container_part_np[
                                                                condition[1][1][0] - 1, 3] <= condition[1][3]):
                                                        flags[index] = True
                                                elif condition[0][0] == 'Сумма масс' and condition[1][
                                                    0] == 'Разность масс':
                                                    weight_sum = 0
                                                    weight_diff = 0
                                                    for ind in condition[0][1]:
                                                        weight_sum += container_part_np[ind - 1, 3]
                                                    for ind in condition[1][1]:
                                                        weight_diff += container_part_np[ind - 1, 3]
                                                    if condition[0][2] <= weight_sum <= condition[0][3] and \
                                                            condition[1][2] <= weight_diff <= condition[1][3]:
                                                        flags[index] = True
                                                elif condition[0][0] == 'Разность масс' and condition[1][
                                                    0] == 'Сумма масс':
                                                    weight_sum = 0
                                                    weight_diff = 0
                                                    for ind in condition[1][1]:
                                                        weight_sum += container_part_np[ind - 1, 3]
                                                    for ind in condition[0][1]:
                                                        weight_diff += container_part_np[ind - 1, 3]
                                                    if condition[1][2] <= weight_sum <= condition[1][3] and \
                                                            condition[0][2] <= weight_diff <= condition[0][3]:
                                                        flags[index] = True
                                            if (np.array(flags) == False).all():
                                                code = str(code)
                                                correct_codes.remove(code)
                                if len(correct_codes) > 0:
                                    flag_for_stop_checking = True
                                    print(f'Для сессии {sessionID}, для платформы {platform} подошло разбиение контейнеров {container_part} по коду {correct_codes}')
        print('success')