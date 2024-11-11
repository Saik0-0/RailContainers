import pyodbc
import numpy as np
from collections import defaultdict


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
containers_array = cursor.fetchall()

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


def sort_SPR(table):
    result = defaultdict(lambda: defaultdict(list))

    data_array = np.array(table, dtype=object)

    for sublist in data_array:
        for item in sublist:
            second_key = item[1]  # Вводим второй элемент - модель - как ключ
            first_elem = item[0]  # Первый элемент - код - для группировки

            result[second_key][first_elem].append(item)  # Добавляем подсписки в структуру

    final_result = defaultdict(dict)

    for second_key, groups in result.items():
        for first_elem, items in groups.items():
            count_key = len(items)  # количество подсписков для данного кода
            if count_key not in final_result[second_key]:
                final_result[second_key][count_key] = []  # создаём новый список для этого количества дубликатов
            final_result[second_key][count_key].extend(items)  # добавляем строки

    # Преобразуем в обычный словарь для дальнейшей работы
    final_result = {k: dict(v) for k, v in final_result.items()}
    for key in final_result:
        final_result[key] = {k: final_result[key][k] for k in sorted(final_result[key])}

    return final_result


#   Создаём сокращенное множество типов платформ P(т.е. уникальные строки без учёта SessionID и carNumber)
P = np.delete(np.array(platforms_array), (0, 1), 1)
P, count_P = np.unique(P, axis=0, return_counts=True)
dupl_P = dict(zip((tuple(map(str, row)) for row in P), map(int, count_P)))
#   P - множество типов платформ
#   dupl_P - словарь {тип платформы: количество таких платформ}

#   Создаём множество всех контейнеров C
C = np.delete(np.array(containers_array), (0, 8), 1)
C = np.unique(C, axis=0)

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

containersSPR = sort_SPR(cont_rules_1)
print(containersSPR)


# """Работаем с таблицей RulesSPR"""
# rules = np.array(containers_on_platforms_rules_array)
# codes = np.unique(rules[:, 0])
# rules_2 = []
# for code in codes:
#     rules_2.append(rules[rules[:, 0] == code].tolist())
#
# rulesSPR = sort_SPR(rules_2)
#
# """Работаем с таблицей TablesSPR"""
# tables = np.array(containers_on_platforms_tables_array)
# codes = np.unique(tables[:, 0])
# tables_new = []
# for code in codes:
#     tables_new.append(tables[tables[:, 0] == code].tolist())
#
# tablesSPR = sort_SPR(tables_new)


cursor.close()
connection.close()
