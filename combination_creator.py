import itertools
import numpy as np


class CombinationCreator:
    def generate_platforms_with_containers(self, platforms_table: list, cont_table: list, sessionID: int):
        platforms_table = np.delete(np.array(platforms_table, dtype=object), 1, 1)
        sessions = np.unique(platforms_table[:, 0])
        if sessionID not in sessions:
            return None

        result = {sessionID: [[], []]}
        result[sessionID][0].extend((platforms_table[platforms_table[:, 0] == sessionID]).tolist())
        result[sessionID][1].extend((cont_table[cont_table[:, 0] == sessionID]).tolist())

        result[sessionID][1] = self.generate_combinations_and_permutations(result[sessionID][1])

        return result

    @staticmethod
    def generate_combinations_and_permutations(cont_table: list):
        cont_combinations = []

        for i in range(1, 4):
            for comb in itertools.combinations(cont_table, i):
                if len(set(np.array(comb)[:, 6])) == 1 and len(set(np.array(comb)[:, 4])) == 1:
                    cont_combinations.extend(itertools.permutations(comb))

        return cont_combinations
