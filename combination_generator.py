from import_data import DataImporter
from modify_data import DataModifier
from combination_creator import CombinationCreator
from checking import CheckingRules


class CombinationGenerator:
    def __init__(self, file_name):
        self.file_name = file_name

    def create(self):
        importer = DataImporter(self.file_name)
        containers_vx = importer.import_table('SELECT * FROM dbo.upContainersVX')
        platforms_vx = importer.import_table('SELECT * FROM dbo.upPlatformsVX')
        containers_spr = importer.import_table('SELECT * FROM dbo.upContainersSPR')
        rules_spr = importer.import_table('SELECT * FROM dbo.upRulesSPR')
        tables_spr = importer.import_table('SELECT * FROM dbo.upTablesSPR')

        modifier = DataModifier()
        transformed_containers_spr = modifier.transform_containers_spr(containers_spr)
        transformed_rules_spr = modifier.transform_rules_spr(rules_spr)
        transformed_tables_spr = modifier.transform_tables_spr(tables_spr)

        combination_creator = CombinationCreator()
        platforms_and_containers = combination_creator.generate_unique_platforms_with_containers(platforms_vx,
                                                                                                 containers_vx)
        checker = CheckingRules()
        result = checker.check_rules(platforms_and_containers, transformed_containers_spr, transformed_rules_spr,
                                     transformed_tables_spr)

        return result

#
# if __name__ == "__main__":
#     generator = CombinationGenerator()
#     generator.create()