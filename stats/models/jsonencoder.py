import decimal
from flask import json


class FlaskJSONEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            # Convert decimal instances to strings.
            return str(obj)
        return super(FlaskJSONEncoder, self).default(obj)
