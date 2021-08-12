import bson


def to_oid_soft(v) -> tuple:
    """
    Attempt to convert v to an ObjectId. Return v if the conversion fails.

    :param v: The value to convert
    :return: A tuple containing the result and whether the conversion was successful
    """
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
