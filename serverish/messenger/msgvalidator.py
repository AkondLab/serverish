from __future__ import annotations

import json
from pathlib import Path
import importlib.resources
from typing import Mapping

import jsonschema


class DictValidator(object):
    def __init__(self, schema: str | dict):
        if isinstance(schema, str):
            self.schema: dict = json.loads(schema)
        else:
            self.schema: dict = schema

    def validate(self, msg: dict | str):
        if isinstance(msg, str):
            msg = json.loads(msg)
        jsonschema.validate(msg, self.schema)


class MetaValidator(DictValidator):
    meta_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "string"},
            "sender": {"type": "string"},
            "receiver": {"type": "string"},
            "ts": {
                "type": "array",
                "items": {"type": "integer"}
            },
            "trace_level": {"type": "integer"},
            "message_type": {"type": "string"},
            "tags": {
                "type": "array",
                "items": {"type": "string"}
            },
        },
        "required": ["id", "ts", "message_type", "tags"]
    }

    def __init__(self):
        super().__init__(self.meta_schema)


class DataValidator(DictValidator):
    def __init__(self, schema: str | dict | None = None, schema_dir: str | Path | None = None, schema_file: str = ''):
        if schema is None:
            schema = self._load_schema(schema_dir, schema_file)
        super().__init__(schema)


    def _load_schema(self, schema_dir: str | Path | None, schema_file: str) -> str:
        if schema_dir is None:
            p = importlib.resources.files(__package__) / '..' / 'schema'
        else:
            p = Path(schema_dir)

        if schema_file == '':
            schema_file = 'default.schema.json'
        elif not schema_file.endswith('.json'):
            schema_file += '.schema.json'

        if schema_dir is None:
            with importlib.resources.as_file(p / schema_file) as fp:
                with open(fp, 'r') as f:
                    schema = f.read()
        else:
            with open(p / schema_file, 'r') as f:
                schema = f.read()
        return schema


class MsgValidator(object):
    def __init__(self, data_validators: Mapping[str, DataValidator] | None = None,
                 meta_validator: MetaValidator | None = None):
        if data_validators is None:
            data_validators = {}
        if meta_validator is None:
            meta_validator = MetaValidator()
        self.data_validators = data_validators
        self.meta_validator = meta_validator

    def validate(self, msg: dict | str):
        if isinstance(msg, str):
            msg = json.loads(msg)
        data, meta = self.split_msg(msg)
        self.meta_validator.validate(meta)
        dataval = self.get_data_validator(meta['message_type'])
        dataval.validate(data)

    def get_data_validator(self, message_type: str | None) -> DataValidator:
        # remove dot and anything after it from message_type (message subtype)
        message_type = message_type.split('.')[0]
        if not message_type:
            message_type = 'default'
        try:
            return self.data_validators[message_type]
        except KeyError:
            self.data_validators[message_type] = DataValidator(schema_file=message_type)
            return self.data_validators[message_type]

    def split_msg(self, msg: dict) -> tuple[dict, dict]:
        data = msg.get('data', {})
        meta = msg.get('meta', {})
        return data, meta