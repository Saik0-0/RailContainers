import itertools
import numpy as np


class CombinationCreator:
    def generate_unique_platforms_with_containers(self, platforms_table, cont_table):

        platforms_table = np.delete(np.array(platforms_table, dtype=object), 1, 1)
        cont_table = np.array(cont_table, dtype=object)
        sessions = np.unique(platforms_table[:, 0])
        result = {}
        for session in sessions:
            if session not in result.keys():
                result[session] = [[], []]
            result[session][0].extend((platforms_table[platforms_table[:, 0] == session]).tolist())
            result[session][1].extend((cont_table[cont_table[:, 0] == session]).tolist())

            result[session][1] = self.generate_combinations_and_permutations_of_cont(result[session][1])

        return result

    def generate_combinations_and_permutations_of_cont(self, cont_table):
        cont_combinations = []

        for i in range(1, 4):
            for comb in itertools.combinations(cont_table, i):
                if len(set(np.array(comb)[:, 6])) == 1 and len(set(np.array(comb)[:, 4])) == 1:
                    cont_combinations.extend(itertools.permutations(comb))

        return cont_combinations
