"""Utilities & constants."""

import json
from datetime import datetime


def log(msg):
    """Log a status message."""
    now = datetime.now().strftime('%Y-%M-%d %H:%M:%S')
    msg = f'{now} {msg}'
    print(msg)


def json_object(df, fields):
    """Build an array of json objects from the dataframe fields."""
    df = df.fillna('')
    json_array = []
    for row in df.itertuples():
        obj = {}
        for field in fields:
            value = getattr(row, field)
            if value != '':
                obj[field] = value
        json_array.append(json.dumps(obj))
    return json_array
