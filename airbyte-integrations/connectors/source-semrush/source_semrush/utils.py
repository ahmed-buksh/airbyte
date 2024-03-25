import csv
import json
from io import StringIO


def csv_to_json(csv_string):
    csv_reader = csv.DictReader(StringIO(csv_string), delimiter=';')
    json_string = json.dumps([row for row in csv_reader], indent=4)
    return json.loads(json_string)
