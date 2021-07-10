from http import HTTPStatus
from typing import Union

import bson


def generate_audio_id():
    return str(bson.ObjectId())


def match_status(expected: Union[int, HTTPStatus], actual: Union[str, int, HTTPStatus]):
    return expected == actual or str(int(expected)) in actual
