from collections import defaultdict
import numpy as np


class DataModifier:
    def transform_containers_spr(self, containers_rules: list):
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

    def transform_rules_spr(self, rules_for_containers: list):
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

    def transform_tables_spr(self, condition_rules: list):
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
                final_result[model][code] = np.array(list(temp_group.values()), dtype=object).transpose(1, 0,
                                                                                                        2).tolist()

        final_result = {k: dict(v) for k, v in final_result.items()}
        return final_result