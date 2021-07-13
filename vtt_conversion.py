from datetime import time
from typing import Tuple, Iterable

import gentle


def aligned_word_to_vtt_cue(word_entry):
    to_symbol = " --> "
    if word_entry.case != "success":
        return
    timestamp_header: str
    start_stamp = seconds_to_isoformat(word_entry.start)
    end_stamp = seconds_to_isoformat(word_entry.end)
    timestamp_header = to_symbol.join([start_stamp, end_stamp])
    speaker_name = "Speaker X"
    speaker_tag = f"<v {speaker_name}>"
    caption = speaker_tag + word_entry.word.upper()
    return "\n".join([timestamp_header, caption])


def gentle_alignment_to_vtt(words: Iterable[gentle.transcription.Word]) -> str:
    vtt = "WEBVTT Kind: captions; Language: en"
    gap = "\n\n"
    for word_entry in words:
        vtt_cue = aligned_word_to_vtt_cue(word_entry)
        if vtt_cue:
            vtt = gap.join([vtt, vtt_cue])
    return vtt


def seconds_to_isoformat(seconds: float) -> str:
    minutes, seconds, microseconds = divide_seconds(seconds)
    return time(minute=minutes, second=seconds, microsecond=microseconds).isoformat()


def divide_seconds(seconds: float) -> Tuple[int, int, int]:
    microseconds = seconds * 1e+6
    minutes = seconds // 60
    return int(minutes % 60), int(seconds % 60), int(microseconds % 1e+6)
