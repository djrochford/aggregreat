"""
Types used internally to check syntax of aggregation stages. I follow the Mongo Aggregation Query
grammar due to Elena Botoeva, Diego Calvanese, Benjamin Cogrel, Martin Rezk, and Guohui Xiao
reasonably closely; see:
https://www.inf.unibz.it/~calvanese/papers/boto-etal-Corr-2016-mongodb-v1.pdf.
"""
from datetime import datetime
from typing import Mapping

from bson.objectid import ObjectId

# Helper functions

def test(value, ag_type):
    """
    Check if a value has the right form to be cast as one of the below aggregation types.
    If it does, return it cast to that type; other wise, return False.
    """
    try:
        typed_value = ag_type(value)
        return typed_value
    except AggregateTypeError:
        return False

def check_pair(pair, expected_type):
    """
    To be used in setter of a class expecting a pair as input.
    """
    if not isinstance(pair, tuple):
        raise AggregateTypeError(f"Expected {pair} to be a(n) {expected_type}, and hence a " +
                                 f"tuple, but it has type {type(pair)}")
    if len(pair) != 2:
        raise AggregateTypeError(f"Expected {pair} to be a(n) {expected_type}, and hence a pair, " +
                                 f"but it has length {len(pair)}")


def enum_setter(val, enums, expected_type):
    """
    Used to set values for enum-like classes.
    """
    if val not in enums:
        raise AggregateTypeError(f"Expected {val} to be a(n) {expected_type} -- i.e., one of " +
                                 f"{enums}.")
    return val


# Error class

class AggregateTypeError(TypeError):
    """
    Raised when coercion to a custom aggregation-type fails, indicating the input is bad.
    """


# Bass validation class

class ValidatedType:
    """
    Base class for these internally-used aggregation types.
    """
    def __init__(self, value):
        self.value = value

# BSON types

class BSONprimitive(ValidatedType):
    """
    For checking values that are supposed to represent primitive BSON data types.
    """
    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        primitive_types = [type(None), bool, int, float, str, datetime, ObjectId]
        if not any([isinstance(val, bson_type) for bson_type in primitive_types]):
            raise AggregateTypeError(f"Expecting {val} to have a bson-equivalent type, but it " +
                                     f"has type {type(val)}.")
        self._value = val

class BSONobject(ValidatedType):
    """
    For checking values that are supposed to represent BSON objects.
    """

    @property
    def value(self):
        return {key: val.value for key, val in self._value.items()}

    @value.setter
    def value(self, val):
        if not isinstance(val, dict):
            raise AggregateTypeError(f"Expecting {val} to reprsent a BSON object, and hence be a " +
                                     f"dict, but it has type {type(val)}.")
        bad_keys = {key for key in val if not isinstance(key, str)}
        if bad_keys:
            raise AggregateTypeError(f"Expecting {val} to represent a BSON object, and hence " +
                                     "have only string keys, but it contains non-string keys " +
                                     f"{bad_keys}.")
        self._value = {key: BSONvalue(v) for key, v in val.items()}


class BSONarray(ValidatedType):
    """
    For checking values that are supposed to represent BSON arrays.
    """
    @property
    def value(self):
        return [item.value for item in self._value]

    @value.setter
    def value(self, val):
        if not isinstance(val, list):
            raise AggregateTypeError(f"Expecting {val} to represent a BSON array, and hence be a " +
                                     f"list, but it has type {type(val)}.")
        self._value = [BSONvalue(item) for item in val]


class BSONvalue(ValidatedType):
    """
    A Tagged Union type for BSON values.
    """

    @property
    def value(self):
        return self._value.value

    @value.setter
    def value(self, val):
        success = False
        for bson_type in [BSONarray, BSONobject, BSONprimitive]:
            try:
                typed_value = bson_type(val)
                success = True
                break
            except AggregateTypeError:
                pass
        if not success:
            raise AggregateTypeError(f"Expected {val} to represent a BSON value, but it's type " +
                                     f"-- viz., {type(val)} -- can not be mapped to a bson type.")
        self._value = typed_value

class ComparisonOperator(ValidatedType):
    """
    Binary operators that compare BSONvalues of the same type.
    """

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        self._value = enum_setter(
            val,
            ["$eq", "$ne", "$gt", "$gte", "$lt", "$lte"],
            "comparison operator"
        )

class ArrayOperator(ValidatedType):
    """
    Operators for determining array membership.
    """
    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        self._value = enum_setter(val, ["$in", "$nin", "$all"], "array operator")

class LogicalOperator(ValidatedType):
    """
    Truth-functional operators on boolean expressions.
    """

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, val):
        self._value = enum_setter(val, ["$and", "$not", "$or"], "logical operator")


class Condition(ValidatedType):
    """
    A condition that can required of a field; appears as the value in the key-value pairs that form
    a query in standard Mongo query language. Stored internally as a pair.
    Can have the following forms:
        ("$exists", <bool>)
        ("$size": <int>)
        ("$not": [<Condition>])
        ("$elemMatch": <Query>)
        (<ComparisonOperator>, <BSONvalue>)
        (<ArrayOperator>: <BSONarray>)
    """

    @property
    def value(self):
        key = self._value[0]
        if key in ["$exists", "$size"]:
            return self._value
        if key == "$elemMatch":
            return ("$elemMatch", self._value[1].value)
        if key == "$not":
            return ("$not", {item.value[0]: item.value[1] for item in self._value[1]})
        return (self._value[0].value, self._value[1].value)

    @value.setter
    def value(self, val):
        check_pair(val, "condition")
        (k, v) = val
        if k == "$exists":
            if not isinstance(v, bool):
                raise AggregateTypeError("An '$exists' condition must have a boolean value, but " +
                                         f"{val} has a value of type {type(v)}")
            self._value = val
        elif k == "$size":
            if not isinstance(v, int):
                raise AggregateTypeError("A '$size' condition must have an integer value, but " +
                                         f"{val} has a value of type {type(v)}")
            self._value = val
        elif k == "$not":
            if not isinstance(v, dict):
                raise AggregateTypeError(f"A '$not' condition must have a dict value, but {val} " +
                                         f"has a value of type {type(v)}.")
            self._value = ("$not", [Condition(item) for item in v.items()])
        elif k == "$elemMatch":
            self._value = ("$elemMatch", Query(v))
        else:
            cop = test(k, ComparisonOperator)
            if cop:
                self._value = (cop, BSONvalue(v))
                return
            aop = test(k, ArrayOperator)
            if aop:
                self._value = (ArrayOperator(k), BSONarray(v))
            else:
                raise AggregateTypeError(f"Expected {val} to be a condition, but it doesn't " +
                                         "have the correct form.")

class Criterion(ValidatedType):
    """
    A key value pair in a query object, in standard Mongo query language, which resolves to a
    boolean. Stored internally as a pair. Can have the following forms:
        (<str>, <BSONvalue>)
        (<str>, [<Condition>]
        (<LogicalOperator>, [<Query>])
    """

    @property
    def value(self):
        (key, val) = self._value
        if isinstance(key, LogicalOperator):
            return (key.value, [v.value for v in val])
        if isinstance(val, BSONvalue):
            return (key, val.value)
        print("=== in criterion getter", val)
        print(val[0].value)

        return (key, {condition.value[0]: condition.value[1] for condition in val})

    @value.setter
    def value(self, val):
        print("=====in criterion", val)
        check_pair(val, "criterion")
        (k, v) = val
        if not isinstance(k, str):
            raise AggregateTypeError(f"Expected {val} to be a criterion, and, hence, for {k} to " +
                                     f"be a string, but it has type {type(val)}.")
        lop = test(k, LogicalOperator)
        if lop:
            if not isinstance(v, list):
                raise AggregateTypeError("Logical-operator criterion must have a list value, " +
                                         f"but {val} has type {type(val)}")
            self._value = (lop, [Query(q) for q in v])
        elif isinstance(v, dict):
            v_keys = v.keys()
            bad_keys = [key for key in v_keys if not isinstance(key, str)]
            if bad_keys:
                raise AggregateTypeError(f"All aggregate dicts must be ")
            operator_keys = [item for item in v_keys if item[0] == "$"]
            if v_keys and len(operator_keys) == len(v_keys):
                self._value = (k, [Condition(c) for c in v.items()])
                return
            if operator_keys:
                raise AggregateTypeError(f"{val} is malformed; should not contain mixture of " +
                                         "operator and non-operator keys.")
        self._value = (k, BSONvalue(v))

class Query(ValidatedType):
    """
    An expression in the standard mongo query language, familiar from `find` and used in $match
    stages. Stored internally as a set of Criteria. Implicitly a conjunction over those Criteria.
    """

    @property
    def value(self):
        return_dict = {}
        for criterion in self._value:
            k, v = criterion.value
            return_dict[k] = v
        return return_dict

    @value.setter
    def value(self, val):
        print("=====in query", val)
        if not isinstance(val, dict):
            raise AggregateTypeError(f"Expected {val} to be a query, and hence a dict, but it " +
                                     f"has type {type(val)}.")
        self._value = {Criterion(pair) for pair in val.items()}

class Stage(ValidatedType):
    """
    A stage in an aggregation pipeline; the top level sub-unit of a pipeline.
    """

    @property
    def value(self):
        key, val = list(self._value.items())[0]
        return {key: val.value}

    @value.setter
    def value(self, val: Mapping[str: Mapping]) -> None:
        print("=======in stage", val)
        if not isinstance(val, dict):
            raise AggregateTypeError(f"Expected {val} to be a stage, and hence a dict, but it " +
                                     f"has type {type(val)}.")
        key_values = list(val.items())
        if len(key_values) != 1:
            raise AggregateTypeError(f"Expected {val} to be a stage, and hence have exactly one " +
                                     "key-value pair.")
        k, v = key_values[0]
        if k == "$match":
            self._value = {"$match": Query(v)}
        else:
            raise NotImplementedError(f"Unsupported stage type {k}")
