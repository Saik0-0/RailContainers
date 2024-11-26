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


def containers_SPR_sort(table):
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


def rules_SPR_sort(table):
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


def tables_SPR_sort(table):
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
    # создаём множество уникальных контейнеров
    # unique_keys, indices, cont_counts = np.unique(cont_table[:, 2:7], axis=1, return_index=True, return_counts=True)
    # print(dict(zip((tuple(np.array(row, dtype=str)) for row in unique_keys), map(int, cont_counts))))
    # result_indices = np.concatenate([np.where((cont_table[:, 2:7] == uk).all(axis=1))[0] for uk, count in zip(unique_keys, cont_counts)])
    # cont_table = cont_table[result_indices]

    cont_combinations = []

    for i in range(1, 3):
        for comb in itertools.combinations(cont_table, i):
            if len(set(np.array(comb)[:, 6])) == 1 and len(set(np.array(comb)[:, 4])) == 1:
                cont_combinations.extend(itertools.permutations(comb))

    # cont_table_length20 = np.array(cont_table)[np.array(cont_table)[:, 2] == '20']
    #
    # if len(cont_table_length20) >= 4:
    #     for comb in itertools.combinations(cont_table_length20, 4):
    #         if len(set(np.array(comb)[:, 7])) == 1 and len(set(np.array(comb)[:, 5])) == 1:
    #             cont_combinations.extend(itertools.permutations(comb))

    # cont_combinations[:0] = map(tuple, cont_table)
    return cont_combinations


# test = [[201, 'CLHU3959860', 20, 21756, 2170, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'HNKU1009723', 20, 21836, 2140, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'HNKU1009739', 20, 21756, 2140, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'HPCU2193814', 20, 21736, 2120, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'HPCU2193835', 20, 21756, 2120, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'XFVU2930684', 20, 21756, 2185, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'XFVU2990087', 20, 21716, 2185, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'XHCU2365092', 20, 21776, 2110, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'XHCU2365106', 20, 21736, 2110, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'BEAU2450597', 20, 21700, 2210, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'TCKU2054460', 20, 21700, 2230, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'TCKU2720628', 20, 21700, 2230, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'HPSU6356362', 40, 10000, 3860, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1975076', 20, 20000, 2180, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'GESU5255760', 40, 10000, 4150, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'RHNU6001497', 40, 10000, 4200, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKHU9416517', 40, 25000, 3840, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1507645', 20, 20970, 2240, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1999442', 20, 20000, 2180, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'TLVU6002136', 40, 10000, 4180, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'CXDU1607197', 20, 22000, 2250, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'DRYU1960164', 20, 22000, 2260, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'HALU2049926', 20, 21500, 2220, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKHU8721239', 40, 26000, 3890, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SEKU4426512', 40, 26000, 3700, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'GESU3332425', 20, 21700, 2200, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'BEAU2990893', 20, 20640, 2210, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'FCIU6200069', 20, 20640, 2100, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'HALU2084423', 20, 20940, 2230, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'HALU2109394', 20, 20640, 2100, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SEGU1477234', 20, 20640, 2180, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1332629', 20, 20640, 2240, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1437506', 20, 20900, 2240, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1628760', 20, 20640, 2240, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1923909', 20, 20640, 2180, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'TCKU3477087', 20, 20640, 2230, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'TEMU0029020', 20, 20640, 2180, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'TEMU4039773', 20, 20640, 2200, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1344255', 20, 21480, 2300, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1514557', 20, 21220, 2240, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1550955', 20, 21060, 2240, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'RHNU6000187', 40, 10000, 4200, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'RHNU4005240', 20, 9000, 2580, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1204269', 20, 21700, 2300, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'PCIU2674368', 20, 21700, 2320, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1417264', 20, 5100, 2300, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'RHNU6001898', 40, 10000, 4200, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'RHNU6000464', 40, 10000, 4200, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'DRYU2825404', 20, 18500, 2200, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1306065', 20, 23700, 2240, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'SKLU1584662', 20, 600, 2240, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'CLHU3526562', 20, 21700, 2220, 'ВНТР', 0, 22, '5-10-B-1', ''], [201, 'SKLU0712964', 20, 21700, 2300, 'ВНТР', 0, 22, '5-6-D-1', ''], [201, 'TCLU6776366', 20, 21700, 2300, 'ВНТР', 0, 22, '5-10-B-2', ''], [201, 'HALU2072016', 20, 21700, 2220, 'ВНТР', 0, 22, '5-6-D-3', ''], [201, 'SKLU1950340', 20, 21700, 2180, 'ВНТР', 0, 22, '5-6-A-2', ''], [201, 'XHCU2358154', 20, 21700, 2110, 'ВНТР', 0, 22, '5-10-A-1', ''], [201, 'WBPU1961584', 20, 22015, 2230, 'ВНТР', 0, 22, '5-6-A-1', ''], [201, 'HALU5681000', 40, 26000, 3890, 'ВНТР', 0, 22, '25-9-C-1', ''], [201, 'HALU5682049', 40, 26000, 3820, 'ВНТР', 0, 22, '25-9-D-1', ''], [201, 'SKHU9945151', 40, 26000, 3700, 'ВНТР', 0, 22, '25-9-C-2', ''], [201, 'HALU2084629', 20, 21700, 2230, 'ВНТР', 0, 22, '5-6-B-2', ''], [201, 'SEGU1814340', 20, 21700, 2180, 'ВНТР', 0, 22, '5-10-A-2', ''], [201, 'SKLU0722026', 20, 21700, 2300, 'ВНТР', 0, 22, '5-6-A-4', ''], [201, 'SKLU1620250', 20, 21700, 2240, 'ВНТР', 0, 22, '5-10-A-3', ''], [201, 'SKLU2061692', 20, 21700, 2180, 'ВНТР', 0, 22, '5-6-C-4', ''], [201, 'SKLU1911087', 20, 21700, 2180, 'ВНТР', 0, 22, '5-10-B-3', ''], [201, 'SKLU2080481', 20, 21700, 2180, 'ВНТР', 0, 22, '5-10-C-1', ''], [201, 'TEMU1226364', 20, 21700, 2180, 'ВНТР', 0, 22, '5-6-D-2', ''], [201, 'SKLU1656063', 20, 21700, 2240, 'ВНТР', 0, 22, '5-6-A-3', ''], [201, 'BMOU4536354', 40, 22300, 3860, 'ВНТР', 0, 22, '25-9-C-3', ''], [201, 'SWFU2003607', 20, 21700, 2200, 'ВНТР', 0, 22, '5-6-C-1', ''], [201, 'BEAU2346026', 20, 21700, 2210, 'ВНТР', 0, 22, '5-6-B-3', ''], [201, 'HALU2042460', 20, 21700, 2220, 'ВНТР', 0, 22, '5-6-C-2', ''], [201, 'SKLU3515613', 20, 21700, 2240, 'ВНТР', 0, 22, '5-6-B-4', ''], [201, 'SKLU2056952', 20, 21700, 2180, 'ВНТР', 0, 22, '5-6-B-1', ''], [201, 'HALU2086529', 20, 23733, 2220, 'ВНТР', 0, 22, '4-2-C-1', ''], [201, 'SKLU0706149', 20, 23776, 2300, 'ВНТР', 0, 22, '4-9-D-2', ''], [201, 'SKLU1420313', 20, 23905, 2300, 'ВНТР', 0, 22, '4-9-C-1', ''], [201, 'SKLU2029920', 20, 23862, 2180, 'ВНТР', 0, 22, '4-9-C-2', ''], [201, 'UETU2232223', 20, 21700, 2180, 'ВНТР', 0, 22, '4-6-C-2', ''], [201, 'JLTU1160713', 20, 21700, 2160, 'ВНТР', 0, 22, '5-6-C-3', ''], [201, 'OOLU1181261', 20, 21700, 2260, 'ВНТР', 0, 22, '4-6-C-1', ''], [201, 'SKHU9934877', 40, 25998, 3700, 'ВНТР', 0, 22, '12-7-B-1', ''], [201, 'SKLU1631255', 20, 21500, 2240, 'ВНТР', 0, 22, '4-6-C-3', '']]
# print(generate_combinations_and_permutations_of_cont((containers_array[containers_array[:, 0] == 17]).tolist()))
# test = {201: [[[[[201, 'dfvdv'], [201, 'dfvsd']], {(201, 'dfvdv'): 2, (201, 'dfvsd'): 4}]], [[201, 'CLHU3959860', 20, 21756, 2170, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'HNKU1009723', 20, 21836, 2140, 'ВНТР', 0, 0, 'O-T-P-R', ''], [201, 'HNKU1009739', 20, 21756, 2140, 'ВНТР', 0, 0, 'O-T-P-R', '']]]}
# print(np.array(test)[np.array(test)[:, 2] == 20])

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
            result[session] = [[[], {}], []]
        result[session][0][0].extend((platforms_table[platforms_table[:, 0] == session]).tolist())
        result[session][0][0], result[session][0][1] = generate_P_and_duplP(result[session][0][0])
        result[session][1].extend((cont_table[cont_table[:, 0] == session]).tolist())

        # print(session)
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
                        [plat_1_1, plat_1_2, ...], 
                        {(plat_1_1): count_1_1, (plat_1_2): count_1_2, ...}
                    ], 
                    [
                        cont_1, cont_2, ..., cont_n, (cont_1, cont_2), (cont_2, cont_1), ..., (cont_n-1, cont_n), ...
                    ]
                ], 
    sessionID_2: 
                [
                    [
                        [plat_2_1, plat_2_2, ...], 
                        {(plat_2_1): count_2_1, (plat_2_2): count_2_2, ...}
                    ], 
                    [
                        cont_1, cont_2, ..., cont_n, (cont_1, cont_2), (cont_2, cont_1), ..., (cont_n-1, cont_n), ...
                    ]
                ], 
    ...
}

"""


"""Функция для прохода по условиям"""


def check_rules(SC, containersSPR_table, rulesSPR_table, tableSPR_table):
    for sessionID, session in SC.items():
        if sessionID < 20:
            # session[0][0] - уникальные типы платформ для данной сессии
            # session[0][1] - словарь с количеством платформ каждого типа
            # session[1] - перестановки контейнеров для данной сессии
            for platform in session[0][0]:
                if platform[1] in list(containersSPR_table.keys()):
                    for container_part in session[1]:
                        correct_codes = []
                        for codes in containersSPR_table[platform[1]][len(container_part)]:
                            """Добавить проверку на классы опасности"""
                            if len(container_part) == len(codes):
                                if (((np.array(container_part, dtype=object)[:, 2]).astype(str) == (np.array(codes, dtype=object)[:, 4]).astype(str)).all()
                                        and ((np.array(container_part, dtype=object)[:, 4]).astype(str) == (np.array(codes, dtype=object)[:, 6]).astype(str)).all()):
                                    correct_codes.append(np.array(codes, dtype=object)[0, 0])
                        container_part_np = np.array(container_part, dtype=object)
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
                            print(f'Для сессии {sessionID}, для платформы {platform} подошло разбиение контейнеров {container_part} по коду {correct_codes}')


#   Создаём множество всех контейнеров C
C = np.array(containers_array)

#   Создаём множество контейнеров, сгруппированных по приоритету отправки
O = np.unique(C[:, 5])  # мн-во приоритетов контейнеров
C_o = []
for o in O:
    C_o_temp = C[C[:, 5] == o]
    C_o.append(C_o_temp.tolist())

#   Создаём множество контейнеров, сгруппированных по партии
B = np.unique(C[:, 6])  # мн-во партий контейнеров
C_b = []
for b in B:
    C_b_temp = C[C[:, 6] == b]
    C_b.append(C_b_temp.tolist())

#   Создаём множество классов опасности грузов
D = set(map(str, np.concatenate([np.array(item.split(',')) if ',' in item else np.array([item])
                                 for item in np.unique(C[:, -1])])))


"""Работаем с таблицей ContainersSPR"""
platforms_rules_1 = np.unique(np.array(containers_rules_array)[:, 1])
cont_rules_1 = []
for platform in platforms_rules_1:
    cont_rules_1.append((np.array(containers_rules_array)[np.array(containers_rules_array)[:, 1] == platform]).tolist())

containersSPR = containers_SPR_sort(cont_rules_1)
# with open('containersSPR.json', 'w', encoding='utf-8') as file:
#     json.dump(containersSPR, file, ensure_ascii=False, indent=4)


"""Работаем с таблицей RulesSPR"""
rules = np.array(containers_on_platforms_rules_array)
codes = np.unique(rules[:, 1])
rules_2 = []
for code in codes:
    rules_2.append(rules[rules[:, 1] == code].tolist())

rulesSPR = rules_SPR_sort(rules_2)
# with open('rulesSPR.json', 'w', encoding='utf-8') as file:
#     json.dump(rulesSPR, file, ensure_ascii=False, indent=4)

"""Работаем с таблицей TablesSPR"""
tables = np.array(containers_on_platforms_tables_array)
codes = np.unique(tables[:, 1])
tables_new = []
for code in codes:
    tables_new.append(tables[tables[:, 1] == code].tolist())

tablesSPR = tables_SPR_sort(tables_new)
# with open('tablesSPR.json', 'w', encoding='utf-8') as file:
#     json.dump(tablesSPR, file, ensure_ascii=False, indent=4)

print(check_rules(SC, containersSPR, rulesSPR, tablesSPR))
# print(list(containersSPR.keys()))
# print(containersSPR['13-470'])
cursor.close()
connection.close()
