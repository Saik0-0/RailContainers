import json
import copy
import yaml
import pyodbc
import numpy as np
from collections import defaultdict
import itertools


SERVER = 'MSI\\SQLEXPRESS'
DATABASE = 'TZ'

connection_string = (f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection'
                     f'=yes;')
connection = pyodbc.connect(connection_string)
cursor = connection.cursor()

"""
Множество всех контейнеров
Значения по столбцам:   
SessionID   
contNumber   
Size    длина
CargoWeight    вес груза(макс)
TareWeight  вес контейнера
Status  состояние
Priority    приоритет
Batch   партия
Stack   
DangerClass класс опасности
"""
containers = "SELECT * FROM dbo.upContainersVX"
cursor.execute(containers)
containers_array = np.array(cursor.fetchall(), dtype=object)
# суммировали массу самого контейнера с массой груза
containers_array[:, 3] += containers_array[:, 4]
containers_array = np.delete(containers_array, 4, axis=1).astype(object)

"""
Множество всех платформ
Значения по столбцам:   
SessionID   вроде как номер отправки
carNumber   
Model   модель
Size    длина
CarryingCapacity    грузоподъёмность
Priority    приоритет
"""
platforms = 'SELECT * FROM dbo.upPlatformsVX'
cursor.execute(platforms)
platforms_array = cursor.fetchall()

"""Варианты размещения контейнеров на моделях платформы
Значения по столбцам:
Код/вариант размещения(один на несколько строчек, сгруппировать по ним)
Модель платформы(также сгруппировать по ним)
Классы опасности, которые могут быть на этой платформе
Номера контейнеров
Длина контейнера
Индекс на платформе куда ставим контейнер
Статус контейнера(куда отправляется)
ID

Связь с отчётом:
Каждая модель платформы  характеризуется множеством вариантов  размещения контейнеров на ней
Хар-ки размещения:
количество контейнеров, на которое рассчитана модель платформы
требуемую длину контейнера для постановки его на i-ую позицию
требуемое состояние контейнера для постановки его на i-ую позицию
множество классов опасности грузов
"""
containers_rules = 'SELECT * FROM dbo.upContainersSPR'
cursor.execute(containers_rules)
containers_rules_array = cursor.fetchall()

"""Требования к контейнерам при выбранном вар-те размещения
Значения по столбцам:
Код/вариант размещения
Модель платформы
Какой критерий рассматриваем
Номера контейнера на платформе(Первый, второй...)
Какое ограничение(<=/>=)
Значение ограничения
ID
"""
containers_on_platforms_rules = 'SELECT * FROM dbo.upRulesSPR'
cursor.execute(containers_on_platforms_rules)
containers_on_platforms_rules_array = cursor.fetchall()

"""Набор правил 'если..., то...' для каждой модели
Нужно транспонировать и сгруппировать по коду размещения
Значения по столбцам:
Код/вариант размещения
Модель платформы
Какой критерий рассматриваем
Номер контейнера на платформе(Первый, второй...) 
Нижняя граница значения
Верхняя граница значения
ID
"""
containers_on_platforms_tables = 'SELECT * FROM dbo.upTablesSPR'
cursor.execute(containers_on_platforms_tables)
containers_on_platforms_tables_array = cursor.fetchall()


"""Функция для сортировки таблицы-правила по моделям и по количеству платформ на ней"""


def containers_SPR_sort(containers_rules: list):
    platforms = np.unique(np.array(containers_rules)[:, 1])
    table = []
    for platform in platforms:
        table.append((np.array(containers_rules)[np.array(containers_rules)[:, 1] == platform]).tolist())

    result = defaultdict(lambda: defaultdict(list))
    table = np.array(table, dtype=object)
    for sublist in table:
        for row in sublist:
            model = row[1]
            code = row[0]
            row[2] = list(row[2].split(','))
            result[model][code].append(row)

    final_result = defaultdict(dict)

    for model, groups in result.items():
        for first_elem, items in groups.items():
            count_key = len(items)  # количество подсписков для данного кода
            if count_key not in final_result[model]:
                final_result[model][count_key] = []  # создаём новый список для этого количества дубликатов
            if len(items) == 1:
                final_result[model][count_key].extend(items)  # добавляем строки
            else:
                final_result[model][count_key].append(items)  # добавляем строки

    # Преобразуем в обычный словарь для дальнейшей работы
    final_result = {k: dict(v) for k, v in final_result.items()}
    for key in final_result:
        final_result[key] = {k: final_result[key][k] for k in sorted(final_result[key])}

    return final_result


def rules_SPR_sort(rules_for_containers: list):
    platforms = np.unique(np.array(rules_for_containers)[:, 1])
    table = []
    for platform in platforms:
        table.append(np.array(rules_for_containers)[np.array(rules_for_containers)[:, 1] == platform].tolist())

    result = defaultdict(lambda: defaultdict(list))
    table = np.array(table, dtype=object)
    for sublist in table:
        for row in sublist:
            model = row[1]
            code = int(row[0])
            row[3] = list(map(int, row[3].split(',')))
            row[5] = float(row[5].replace(',', '.')) * 1000
            result[model][code].append(row[2:])

    result = {k: dict(v) for k, v in result.items()}
    return result


def tables_SPR_sort(condition_rules: list):
    platforms = np.unique(np.array(condition_rules)[:, 1])
    table = []
    for platform in platforms:
        table.append(np.array(condition_rules)[np.array(condition_rules)[:, 1] == platform].tolist())

    result = defaultdict(lambda: defaultdict(list))
    table = np.array(table, dtype=object)
    for sublit in table:
        for row in sublit:
            model = row[1]
            code = int(row[0])
            row[3] = tuple(map(int, row[3].split(',')))
            row[4] = float(row[4].replace(',', '.')) * 1000
            row[5] = float(row[5].replace(',', '.')) * 1000
            result[model][code].append(row[2:])

    final_result = defaultdict(lambda: defaultdict(list))

    for model, groups in result.items():
        for code, rows in groups.items():
            temp_group = defaultdict(list)
            for row in rows:
                temp_group[tuple(row[:2])].append(row)
            final_result[model][code] = np.array(list(temp_group.values()), dtype=object).transpose(1, 0, 2).tolist()

    final_result = {k: dict(v) for k, v in final_result.items()}
    return final_result


def generate_combinations_and_permutations_of_cont(cont_table):
    cont_combinations = []

    for i in range(1, 4):
        for comb in itertools.combinations(cont_table, i):
            if len(set(np.array(comb)[:, 6])) == 1 and len(set(np.array(comb)[:, 4])) == 1:
                cont_combinations.extend(itertools.permutations(comb))
                # cont_combinations.extend(comb)

    return cont_combinations


"""Функция разделения таблицы платформ на уникальные платформы(типы) и количество таких типов в таблице
Структура:
[plat_1, plat_2, ...], {(plat_1): count_1, (plat_2): count_2, ...}"""


def generate_P_and_duplP(platforms_table):
    plats, count_platforms = np.unique(platforms_table, axis=0, return_counts=True)
    dupl_platforms = dict(zip((tuple(np.array(row, dtype=str)) for row in plats), map(int, count_platforms)))
    return plats.tolist(), dupl_platforms


"""Функция разделения входных таблиц контейнеров и платформ на сессии и разделение таблицы платформ на типы и количество
таких типов
Структура:
{sessionID_1: [[[plat_1_1, plat_1_2, ...], {(plat_1_1): count_1_1, (plat_1_2): count_1_2, ...}], [conts]], 
sessionID_2: [[[plat_2_1, plat_2_2, ...], {(plat_2_1): count_2_1, (plat_2_2): count_2_2, ...}], [conts]], ...}"""


def P_and_C_test(platforms_table, cont_table):
        platforms_table = np.delete(np.array(platforms_table, dtype=object), 1, 1)
        cont_table = np.array(cont_table, dtype=object)
        sessions = np.unique(platforms_table[:, 0])
        result = {}
        for session in sessions:
            if session not in result.keys():
                result[session] = [[], []]
            result[session][0].extend((platforms_table[platforms_table[:, 0] == session]).tolist())
            result[session][1].extend((cont_table[cont_table[:, 0] == session]).tolist())

            result[session][1] = generate_combinations_and_permutations_of_cont(result[session][1])

        return result


"""Генерация платформ и перестановок контейнеров для них для каждого кода"""
SC = P_and_C_test(platforms_array, containers_array)
# with open('SC.yaml', 'w', encoding='utf-8') as file:
#     yaml.dump(SC, file, allow_unicode=True)
#
# with open('SC.yaml', 'r', encoding='utf-8') as file:
#     SC = yaml.safe_load(file)
#
# print(SC)
"""Структура SC:
{
    sessionID_1: 
                [
                    [
                        plat_1_1, plat_1_2, ...
                    ], 
                    [
                        cont_1, cont_2, ..., cont_n, (cont_1, cont_2), (cont_2, cont_1), ..., (cont_n-1, cont_n), ...
                    ]
                ], 
    sessionID_2: 
                [
                    [
                        plat_2_1, plat_2_2, ...
                    ], 
                    [
                        cont_1, cont_2, ..., cont_n, (cont_1, cont_2), (cont_2, cont_1), ..., (cont_n-1, cont_n), ...
                    ]
                ], 
    ...
}

"""


"""Функция для прохода по условиям"""


def check_rules(SC: dict, containersSPR_table: dict, rulesSPR_table: dict, tableSPR_table: dict):
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
                                    if (((np.array(container_part, dtype=object)[:, 2]).astype(str) == (np.array(codes, dtype=object)[:, 4]).astype(str)).all()
                                            and ((np.array(container_part, dtype=object)[:, 4]).astype(str) == (np.array(codes, dtype=object)[:, 6]).astype(str)).all()):
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
                                                weight_diff = abs(container_part_np[row[1][0] - 1, 3] - container_part_np[row[1][1] - 1, 3])
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
                                                if (condition[0][2] <= container_part_np[condition[0][1][0] - 1, 3] <= condition[0][3]
                                                        and condition[1][2] <= container_part_np[condition[1][1][0] - 1, 3] <= condition[1][3]):
                                                    flags[index] = True
                                            elif condition[0][0] == 'Сумма масс' and condition[1][0] == 'Разность масс':
                                                weight_sum = 0
                                                weight_diff = 0
                                                for ind in condition[0][1]:
                                                    weight_sum += container_part_np[ind - 1, 3]
                                                for ind in condition[1][1]:
                                                    weight_diff += container_part_np[ind - 1, 3]
                                                if condition[0][2] <= weight_sum <= condition[0][3] and condition[1][2] <= weight_diff <= condition[1][3]:
                                                    flags[index] = True
                                            elif condition[0][0] == 'Разность масс' and condition[1][0] == 'Сумма масс':
                                                weight_sum = 0
                                                weight_diff = 0
                                                for ind in condition[1][1]:
                                                    weight_sum += container_part_np[ind - 1, 3]
                                                for ind in condition[0][1]:
                                                    weight_diff += container_part_np[ind - 1, 3]
                                                if condition[1][2] <= weight_sum <= condition[1][3] and condition[0][2] <= weight_diff <= condition[0][3]:
                                                    flags[index] = True
                                        if (np.array(flags) == False).all():
                                            code = str(code)
                                            correct_codes.remove(code)
                            if len(correct_codes) > 0:
                                flag_for_stop_checking = True
                            #     print(f'Для сессии {sessionID}, для платформы {platform} подошло разбиение контейнеров {container_part} по коду {correct_codes}')
    print('success')


"""Работаем с таблицей ContainersSPR"""
containersSPR = containers_SPR_sort(containers_rules_array)
# with open('containersSPR.json', 'w', encoding='utf-8') as file:
#     json.dump(containersSPR, file, ensure_ascii=False, indent=4)


"""Работаем с таблицей RulesSPR"""
rulesSPR = rules_SPR_sort(containers_on_platforms_rules_array)
# with open('rulesSPR.json', 'w', encoding='utf-8') as file:
#     json.dump(rulesSPR, file, ensure_ascii=False, indent=4)

"""Работаем с таблицей TablesSPR"""
tablesSPR = tables_SPR_sort(containers_on_platforms_tables_array)
# with open('tablesSPR.json', 'w', encoding='utf-8') as file:
#     json.dump(tablesSPR, file, ensure_ascii=False, indent=4)

print(check_rules(SC, containersSPR, rulesSPR, tablesSPR))

cursor.close()
connection.close()
