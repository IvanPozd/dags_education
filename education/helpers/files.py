import json


def get_json_data_from_source(source_file_path: str, mode: str = "r"):
    with open(source_file_path, mode=mode) as json_file:
        return json.load(json_file)
