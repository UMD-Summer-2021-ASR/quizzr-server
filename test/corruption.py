# Generator function that produces dictionaries with each key missing.
# depth is for recursively "shattering" dictionaries. By default, depth is 0, meaning no recursive shattering will take
# place. A depth of 1, for example, also generates dictionaries with its child dictionaries shattered.
def shatter_dict(dictionary: dict, depth=0):
    # TODO:
    #  "exclude" argument. List of items. Use ("<key>", List) for ignoring entries within a dictionary.
    #  Use ("<key>", "all") for ignoring all entries within the dictionary.
    # TODO:
    #  "include" argument. Similar structure to ignoreKeys but only shatters certain keys.
    for key in dictionary.keys():
        next_dict = dictionary.copy()

        if depth != 0 and type(next_dict[key]) is dict:
            for entry in shatter_dict(next_dict[key], depth - 1):
                next_dict2 = next_dict.copy()
                next_dict2[key] = entry
                yield next_dict2

        del next_dict[key]
        yield next_dict


# if __name__ == '__main__':
#     test_dict = {
#         "key1": {"foo": {"a": "int", "b": "string", "c": "list"}, "bar": "b", "baz": "c"},
#         "key2": {"a": "int", "b": "string", "c": "list"},
#         "key3": "value3"
#     }
#     for depth_num in [0, 1, 2, -1]:
#         print(f"depth = {depth_num}")
#         for dictionary in shatter_dict(test_dict, depth=depth_num):
#             print(dictionary)
