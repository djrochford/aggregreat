"""
Tests for validation procedure.
"""

from datetime import date, datetime, timedelta

from pytest import raises
from bson.objectid import ObjectId

from validate import is_valid_pipeline, AggregreatTypeError

def test_match_stage():
    """
    Tests $match stages
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
        assert is_valid_pipeline([{"$match": query}]) is True

    bad_bson_value = {"someField": date}
    with raises(AggregreatTypeError) as err:
        is_valid_pipeline([{"$match": bad_bson_value}])
        assert err.value == (f"Expected {bad_bson_value} to be a BSON value, but it has " +
                             f"non-valid type {type(bad_bson_value)}.")

    mixed_dollar_keys = {"someField": {"$in": ["dog, cat"], "tree": 2}}
    with raises(AggregreatTypeError) as err:
        is_valid_pipeline([{"$match": mixed_dollar_keys}])
        assert err.value == (f"Bad dictionary {mixed_dollar_keys}. Cannot mix operator keys " +
                             "(beginning with '$') with non-operator keys.")