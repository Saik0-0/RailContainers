import json


class DataExporter:
    def export_result(self, sessionID: int, result_dict: dict):
        with open("result_session_" + str(sessionID) + ".json", 'w', encoding='utf-8') as result_file:
            json.dump(result_dict, result_file, ensure_ascii=False, indent=4)

