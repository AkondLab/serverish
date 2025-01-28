import jsonschema
import json

from serverish.messenger import Messenger
from serverish.messenger.msgvalidator import DataValidator, MsgValidator


def test_dv_default():
    dv = DataValidator()
    dv.validate({'a': 1, 'b': 2})

def test_msgv_default():
    msgv = MsgValidator()
    msg = Messenger().create_msg(data={'a': 1, 'b': 2})
    msgv.validate(msg)

def test_schema_telemtry():
    dv = DataValidator(schema_file='telemetry.schema.json')

    data = """
    {"ts": [2025, 1, 22, 15, 55, 12, 98582], "version": "1.0", "measurements": {"temperature_C": 16.5, "humidity": 20, "wind_dir_deg": 352, "wind_ms": 6.25856, "wind_10min_ms": 7.59968, "pressure_Pa": 729.6990298933405, "bar_trend": 0, "rain_mm": 0.0, "rain_day_mm": 0.0, "indoor_temperature_C": 19.833333333333332, "indoor_humidity": 15}}
    """
    #     {
    #     "ts": [2025, 1, 22, 15, 55, 12, 98582],
    #     "version": "1.0",
    #     "measurements": {
    #         "temperature_C": 16.5,
    #         "humidity": 20,
    #         "wind_dir_deg": 352,
    #         "wind_ms": 6.25856,
    #         "wind_10min_ms": 7.59968,
    #         "pressure_Pa": 729.6990298933405,
    #         "bar_trend": 0,
    #         "rain_mm": 0.0,
    #         "rain_day_mm": 0.0,
    #         "indoor_temperature_C": 19.833333333333332,
    #         "indoor_humidity": 15
    #     }
    # }

    dv.validate(data)

    data_bad = """
    {"ts": [2025, 1, 22, 15, 55, 12, 98582], "version": "1.0", "errors": {"R1": {}}, "measurements": {"temperature_C": 16.5, "humidity": 20, "wind_dir_deg": 352, "wind_ms": 6.25856, "wind_10min_ms": 7.59968, "pressure_Pa": 729.6990298933405, "bar_trend": 0, "rain_mm": 0.0, "rain_day_mm": 0.0, "indoor_temperature_C": 19.833333333333332, "indoor_humidity": 15}}
    """
    try:
        dv.validate(data_bad)
        assert False
    except jsonschema.exceptions.ValidationError:
        pass




