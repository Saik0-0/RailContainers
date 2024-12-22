import itertools
import numpy as np


class CombinationCreator:
    def generate_unique_platforms_with_containers(self, platforms_table, cont_table, sessionID: int):
        platforms_table = np.delete(np.array(platforms_table, dtype=object), 1, 1)
        cont_table = np.array(cont_table, dtype=object)
        sessions = np.unique(platforms_table[:, 0])
        if sessionID not in sessions:
            return None

        result = {}
        if sessionID not in result.keys():
            result[sessionID] = [[], []]
        result[sessionID][0].extend((platforms_table[platforms_table[:, 0] == sessionID]).tolist())
        result[sessionID][1].extend((cont_table[cont_table[:, 0] == sessionID]).tolist())

        result[sessionID][1] = self.generate_combinations_and_permutations_of_cont(result[sessionID][1])

        return result

    def generate_combinations_and_permutations_of_cont(self, cont_table):
        cont_combinations = []

        for i in range(1, 4):
            for comb in itertools.combinations(cont_table, i):
                if len(set(np.array(comb)[:, 6])) == 1 and len(set(np.array(comb)[:, 4])) == 1:
                    cont_combinations.extend(itertools.permutations(comb))

        return cont_combinations
