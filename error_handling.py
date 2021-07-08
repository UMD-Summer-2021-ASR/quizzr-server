import bson


def to_oid_soft(v) -> tuple:
    if any([type(v) is dtype for dtype in [str, bytes, bson.ObjectId]]):
        try:
            result = bson.ObjectId(v)
            success = True
        except bson.errors.InvalidId:
            result = v
            success = False
    else:
        result = v
        success = False
    return result, success
