from jsonschema import validate, RefResolver, ValidationError, draft7_format_checker
from json import JSONEncoder
from datetime import datetime
from enum import Enum
from pandas import DataFrame
from collections.abc import Callable
from scheduler.enums import Fields, Status, RunType, ExceptionHandlerPolicy, DateModifierPolicy, ObjectType, TriggerType

ENUMS = frozenset({"Fields": Fields,
                   "Status": Status,
                   "RunType": RunType,
                   "ExceptionHandlerPolicy": ExceptionHandlerPolicy,
                   "DateModifierPolicy": DateModifierPolicy,
                   "ObjectType": ObjectType,
                   "TriggerType": TriggerType})

def validate_json(json_data, schema, schema_path):
    """
    Validating the given json data based on the schema provided.
    """
    try:
        validate(instance=json_data, schema=schema, resolver=RefResolver(schema_path, schema),
                 format_checker=draft7_format_checker)
        return True
    except ValidationError as err:
        print(err)
        return False

class SchedulerEncoder(JSONEncoder):
    def default(self, obj):
        #if isinstance(obj, Scheduler):
        #attr_dict = obj.__dict__

        #return attr_dict

        # Let the base class default method raise the TypeError
        return JSONEncoder.default(self, obj)

def to_json(value):
    new_value = value

    if hasattr(value, "__getstate__"):
        new_value = value.__getstate__()

    if isinstance(value, datetime):
        new_value = {"Datetime": value.isoformat()}

    if isinstance(value, Enum):
        new_value = {"Enum": str(value)}

    if isinstance(value, DataFrame):
        new_value = {"DataFrame": value.to_dict()}

    if isinstance(value, Callable):
        new_value = {"Callable": None}

    if isinstance(value, list):
        new_value = value.copy()
        for i, subvalue in enumerate(value):
            new_value[i] = to_json(subvalue)

    if isinstance(value, dict):
        new_value = value.copy()
        for key, subvalue in value.items():
            new_value[key] = to_json(subvalue)

    return new_value

def from_json(value):
    new_value = value
    if isinstance(value, dict):
        if "Datetime" in value:
            new_value = datetime.fromisoformat(value["Datetime"])
        elif "Enum" in value:
            name, member = value["Enum"].split(".")
            new_value = ENUMS[name][member]
        elif "DataFrame" in value:
            new_value = DataFrame.from_dict(value["DataFrame"])
        elif "Callable" in value:
            new_value = None
        else:
            new_value = value.copy()
            for key, subvalue in value.items():
                new_value[key] = from_json(subvalue)

    if isinstance(value, list):
        new_value = value.copy()
        for i, subvalue in enumerate(value):
            new_value[i] = from_json(subvalue)

    if hasattr(value, "__setstate__"):
        new_value = type(value).__setstate__(value)

    return new_value




def getstate_type_handler(func):
    def wrapper(*args, **kwargs):

        attr_dict = func(*args, **kwargs)

        for key, value in attr_dict.items():
            attr_dict[key] = to_json(value)



        return attr_dict

    return wrapper

def setstate_type_handler(func):
    def wrapper(self, attr_dict):

        for key, value in attr_dict.items():
            attr_dict[key] = from_json(value)


        func(self, attr_dict)

        return None

    return wrapper

