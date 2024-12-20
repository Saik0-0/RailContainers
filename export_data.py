import json


class DataExporter:
    def export_result(self, result_dict: dict):
        with open("result.json", 'w', encoding='utf-8') as result_file:
            json.dump(result_dict, result_file, ensure_ascii=False, indent=4)
