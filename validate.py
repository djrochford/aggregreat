"""
Aggregation pipeline validation code.
"""

from datetime import datetime
from typing import Union, List, Dict, Tuple

from bson.objectid import ObjectId

# input types -- more for readability than anything else.

BSONprimitive = Union[type(None), bool, int, float, str, datetime, ObjectId]
BSONarray = List["BSONvalue"]
BSONobject = Dict[str, "BSONvalue"]
BSONvalue = Union[BSONprimitive, BSONobject, BSONarray]

ExpressionObject = Dict[str, "Expression"]
Expression = Union[BSONvalue, ExpressionObject]

ExpressionPair = Tuple[str, ExpressionObject]


#The error that the validation procedures will raise

class AggregreatTypeError(TypeError):
    """
    Raised to indicate an invalid aggregation pipeline.
    """

# helper procedures

def has_dollar_keys(dictionary: Dict) -> bool:
    """
    Raises an exception if not all keys are strings.
    Returns True if all keys begin with `$`.
    Returns False is all keys do not begin with `$`.
    Raises an error if some keys begin with `$` and some do not.
    """
    non_string_keys = [key for key in dictionary if not isinstance(key, str)]
    if non_string_keys:
         raise AggregreatTypeError("Aggregation-language dicts cannot contain non-string keys, " +
                                   f"but {dictionary} contains the following: {non_string_keys}")
    dollar_keys = [key for key in dictionary if isinstance(key, str)
                   and key and key[0] == "$"]
    if dollar_keys:
        if len(dollar_keys) != len(dictionary):
            raise AggregreatTypeError(f"Bad dictionary {dictionary}. Cannot mix operator keys " + 
                                      "(beginning with '$') with non-operator keys.")
        return True
    return False

# validation procedures

def is_valid_condition(condition: ExpressionPair) -> None:
    """
    A condition is a one-place predicate, formed from a relation-term and a relatum-term, which can
    be of various types. A criterion is a condition applied to a field.
    Can have the following forms:
    ("$exists", <bool>)
    ("$size": <int>)
    ("$not": <Condition>)
    ("$elemMatch": <Query>)
    ("$elemMatch": <Condition>)
    (<ComparisonOperator>, <BSONvalue>)
    (<ArrayOperator>, <BSONarray>)
    """

    key, value = condition
    if key == "$exists":
        if not isinstance(value, bool):
            raise AggregreatTypeError(f"`$exists` requires a bool argument, but {value} has type " +
                                      f"{type(value)}.")
    elif key == "$size":
        if not isinstance(value, int):
            raise AggregreatTypeError(f"`$size` requires an int argument, but {value} has type " +
                                      f"{type(value)}.")
    elif key == "$not":
        if not isinstance(value, Dict):
            raise AggregreatTypeError(f"`$not` requires a dict argument, but {value} has type " +
                                      f"{type(value)}.")
        pairs = value.items()
        if len(pairs) != 1:
            raise AggregreatTypeError(f"{value} is a bad `$not` argument; `$not requires an " +
                                      "argument with exactly one key-value pair.")
        is_valid_condition(list(pairs)[0])
    elif key == "$elemMatch":
        if not isinstance(value, Dict):
            raise AggregreatTypeError(f"`$elemMatch` requires a dict argument, but {value} has " +
                                      f"type {type(value)}.")
        if has_dollar_keys(value):
            for pair in value.items():
                is_valid_condition(pair)
        else:
            is_valid_query(value)
    elif key in ["$eq", "$ne", "$gt", "$gte", "$lt", "$lte"]:
        is_valid_bson(value)
    elif key in ["$in", "$nin"]:
        if not isinstance(value, List):
            raise AggregreatTypeError(f"{key} requires a list argument, but {value} has type " +
                                      f"{type(value)}.")
        for item in value:
            is_valid_bson(item)
    else:
        raise AggregreatTypeError(f"Unknown operator {key}.")


def is_valid_bson(bson_value: BSONvalue) -> None:
    """
    Checks that input is of a python type that is mapped (in pymongo) to a BSON type.
    """
    primitive_types = [type(None), bool, int, float, str, datetime, ObjectId]
    is_primitive_type = any([isinstance(bson_value, bson_type) for bson_type in primitive_types])
    if not is_primitive_type:
        if isinstance(bson_value, List):
            for item in bson_value:
                is_valid_bson(item)
        elif isinstance(bson_value, Dict):
            if has_dollar_keys(bson_value):
                raise AggregreatTypeError(f"Expected {bson_value} to be a BSON object, but it " +
                                          "has keys beginning with `$`.")
            values = bson_value.values()
            for value in values:
                is_valid_bson(value)
        else:
            raise AggregreatTypeError(f"Expected {bson_value} to be a BSON value, but it has " +
                                      f"non-valid type {type(bson_value)}.")

def is_valid_path(path: str) -> None:
    """
    Checks a field-path, used to refer to a field on the input documents.
    """
    if not isinstance(path, str):
        raise AggregreatTypeError(f"Expected {path} to be a path-expression, and hence a string, " +
                                  f"but it has type {type(path)}.")


def is_valid_criterion(criterion: ExpressionPair) -> None:
    """
    A crtierion is a key-value pair which evaluates to a boolean. A query is an implicit conjunction
    over its consitutent criteria.
    Three forms:
    (<LogicalOperator>, [<Query>])
    (str, {<Condition>})
    (str, <BSONvalue>)
    """
    key, value = criterion
    if not isinstance(key, str):
        raise AggregreatTypeError("Keys in aggregate key-value expressions must be strings, but " +
                                  f"{key} has type {type(key)}.")
    if key in ["$and", "$or", "$nor"]:
        if not isinstance(value, List):
            raise AggregreatTypeError(f"{key} is a logical operator, and requires a list-type " +
                                      f"value, but {value} has type {type(value)}.")
        for item in value:
            is_valid_query(item)
    else:
        is_valid_path(key)
        if isinstance(value, Dict):
            if has_dollar_keys(value):
                for pair in value.items():
                    is_valid_condition(pair)
                    return
        is_valid_bson(value)



def is_valid_query(query: ExpressionObject) -> None:
    """
    Checks query in the standard Mongo query language, familiar from `find`, and used in `$match`
    stages. A query is a set of criteria, over which the query is an implicit conjunction.
    """
    if not isinstance(query, dict):
        raise AggregreatTypeError(f"Expected {query} to be a query, and hence a dict, but it has " +
                                  f"type {type(query)}.")
    key_pairs = query.items()
    for pair in key_pairs:
        is_valid_criterion(pair)


def is_valid_stage_type(stage_type: str) -> None:
    """
    Checks the tag that indicate a type of stage.
    """
    valid_stage_types = ["$match"]
    if not stage_type in valid_stage_types:
        raise AggregreatTypeError(f"{stage_type} is not a valid stage-type.")

def is_valid_stage(stage: ExpressionObject) -> None:
    """
    Stages are the largest-scale building blocks of an aggregation pipeline.
    """
    if not isinstance(stage, Dict):
        raise AggregreatTypeError(f"Expected {stage} to be a stage expression, and hence a dict, " +
                                  f"but it has type {type(stage)}.")
    key_pairs = stage.items()
    if len(key_pairs) != 1:
        raise AggregreatTypeError(f"Expected {stage} to be a stage expression, and hence have" +
                                  "exactly one key-value pair.")
    stage_type, stage_spec = list(key_pairs)[0]
    is_valid_stage_type(stage_type)
    if stage_type == "$match":
        is_valid_query(stage_spec)

def is_valid_pipeline(pipeline: List[ExpressionObject]) -> bool:
    """
    Outermost validation procedure; this is what should be imported by the user.
    """
    if not isinstance(pipeline, List):
        raise AggregreatTypeError(f"Expected {pipeline} to be an aggregation pipeline, and hence " +
                                  f"a list, but it has type {type(pipeline)}.")
    for stage in pipeline:
        is_valid_stage(stage)
    return True
