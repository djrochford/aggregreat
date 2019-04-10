"""
Tests for the Pipeline class.
"""

from datetime import date, datetime, timedelta
import pytest

from bson.objectid import ObjectId

from pipeline import Pipeline
from aggregation_types import AggregateTypeError

def test_pipeline_instantiation_and_pop():
    """
    Test instantiation of Pipeline class, and the pop-method.
    """
    pipey = Pipeline()
    assert pipey.pop() == []

def test_good_queries():
    """
    Test Pipeline's `match` method, and direct Pipeline instantion on `$match` dicts,
    with well-formed queries.
    """
    good_queries = [
        #BSON primitives
        {"someField": None},
        {"someField": True},
        {"someField": 2},
        {"someField": 2.5},
        {"someField": "someValue"},
        {"someField": datetime.now()},
        {"someField": ObjectId()},
        #BSON object
        {"someField": {}},
        {"someField": {"someSubfield": True}},
        {"someField": {"subField1": 1, "subfield2": 1.57}},
        {"someField": {"subField": {"subsubfield": "someValue"}}},
        #BSON array
        {"someField": []},
        {"someField": [datetime.now(), ObjectId(), None, True]},
        #several BSON values
        {"someField": {"subField": True}, "someOtherField": "someValue"},
        #comparison
        {"height": {"$gte": 98}},
        {"created": {"$gt": datetime.now() - timedelta(days=1), "$lte": datetime.now()}},
        #array operator
        {"_id": {"$in": [ObjectId(), ObjectId()]}},
        {"field.subfield": {"$nin": ["dog", "tree", 3]}},
        #logical operator
        {"$and": [{"workflow": "shameless"}, {"dialog": "self-promotion"}]},
        {"$or": [
            {"$nor": [
                {"workflow": "shameless", "dialog": "self-promotion"},
                {"_id": {"$in": [ObjectId(), "cat"]}}
            ]},
            {"splendid": {"$ne": 3}}
        ]},
        #exists condition
        {"_contactSettings.contacted": {"$exists": True}},
        #size condition
        {"deliveryFailureUsers": {"$size": 0}},
        #not condition
        {"someField": {"$not": {"$size": 2}}},
        #elemMatch condition
        {"someField": {"$elemMatch": {"created": {
            "$gt": datetime.now() - timedelta(days=1),
            "$lte": datetime.now()
        }}}}

    ]

    for query in good_queries:
        assert Pipeline().match(query=query).pop() == [{"$match": query}]
        assert Pipeline([{"$match": query}]).pop() == [{"$match": query}]

def test_bad_queries():
    """
    Test that Pipeline raises an exception, and the right one, for ill-formed queries.
    """
    bad_primitive = date.today()
    with pytest.raises(AggregateTypeError) as err:
        Pipeline().match(query={"someField": bad_primitive})
        assert err.value == (f"Expecting {bad_primitive} to have a bson-equivalent type, but it " +
                             f"has type {type(bad_primitive)}.")

    bad_object = {3: "dog"}
    with pytest.raises(AggregateTypeError) as err:
        Pipeline().match(query={"someField": bad_object})
        assert err.value == "something"
