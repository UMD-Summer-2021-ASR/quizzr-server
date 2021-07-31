from datetime import time
from typing import Tuple, Iterable

import gentle


def aligned_word_to_vtt_cue(word_entry):
    """Convert a Gentle Word object into a VTT cue."""
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
    """Convert a series of Gentle Word objects into a VTT string."""
    vtt = "WEBVTT Kind: captions; Language: en"
    gap = "\n\n"
    for word_entry in words:
        vtt_cue = aligned_word_to_vtt_cue(word_entry)
        if vtt_cue:
            vtt = gap.join([vtt, vtt_cue])
    return vtt


def seconds_to_isoformat(seconds: float) -> str:
    """Convert seconds into a properly-formatted ISO timestamp (00:MM:SS.FFFFFF)"""
    minutes, seconds, microseconds = divide_seconds(seconds)
    return time(minute=minutes, second=seconds, microsecond=microseconds).isoformat()


def divide_seconds(seconds: float) -> Tuple[int, int, int]:
    """Split up seconds into a tuple of minutes, seconds, and microseconds.
    Postcondition: 0 <= minutes < 60, 0 <= seconds < 60, 0 <= microseconds < 1000000"""
    microseconds = seconds * 1e+6
    minutes = seconds // 60
    return int(minutes % 60), int(seconds % 60), int(microseconds % 1e+6)
