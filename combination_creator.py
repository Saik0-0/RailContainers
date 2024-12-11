import itertools
import numpy as np


class CombinationCreator:
    def generate_platforms_and_combinations(self, platforms, containers):
        def generate_combinations_and_permutations_of_cont(cont_table):
            cont_combinations = []

            for i in range(1, 4):
                for comb in itertools.combinations(cont_table, i):
                    if len(set(np.array(comb)[:, 6])) == 1 and len(set(np.array(comb)[:, 4])) == 1:
                        cont_combinations.extend(itertools.permutations(comb))
                        # cont_combinations.extend(comb)

            return cont_combinations

        def generate_P_and_duplP(platforms_table):
            plats, count_platforms = np.unique(platforms_table, axis=0, return_counts=True)
            dupl_platforms = dict(zip((tuple(np.array(row, dtype=str)) for row in plats), map(int, count_platforms)))
            return plats.tolist(), dupl_platforms

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

        return P_and_C_test(platforms, containers)