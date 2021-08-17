import collections.abc


def deep_update(d, u):
    """
    Apply an update operation to a dictionary without overwriting embedded dictionaries.

    Source: https://stackoverflow.com/questions/3232943/update-value-of-a-nested-dictionary-of-varying-depth

    :param d: The base dictionary
    :param u: The dictionary to merge on top of the base
    :return: The base dictionary, for recursion purposes
    """
    for k, v in u.items():
        if isinstance(v, collections.abc.Mapping):
            d[k] = deep_update(d.get(k, {}), v)
        else:
            d[k] = v
    return d
