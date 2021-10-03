import string
from typing import Tuple, Iterable

from gentle import transcription


def aligned_word_to_vtt_cue(word_entry: transcription.Word, speaker_name="Speaker 0"):
    """
    Convert a Gentle Word object into a VTT cue.

    :param word_entry: A Gentle Word object
    :param speaker_name: (optional) The name of the speaker to display
    :return: A VTT cue including a timestamp and the word aligned
    """
    to_symbol = " --> "
    if word_entry.case != "success":
        return
    timestamp_header: str
    start_stamp = seconds_to_vtt_timestamp(word_entry.start)
    end_stamp = seconds_to_vtt_timestamp(word_entry.end)
    timestamp_header = to_symbol.join([start_stamp, end_stamp])
    speaker_tag = f"<v {speaker_name}>"
    caption = speaker_tag + word_entry.word
    return "\n".join([timestamp_header, caption])


def gentle_alignment_to_vtt(words: Iterable[transcription.Word],
                            header="WEBVTT Kind: captions; Language: en") -> str:
    """
    Convert a series of Gentle Word objects into a VTT string.

    :param words: Any kind of iterable that contains Gentle Word objects
    :param header: (optional) The string to use at the top of the VTT file
    :return: A VTT-converted forced alignment
    """
    vtt = header
    gap = "\n\n"
    for word_entry in words:
        vtt_cue = aligned_word_to_vtt_cue(word_entry)
        if vtt_cue:
            vtt = gap.join([vtt, vtt_cue])
    return vtt


def seconds_to_vtt_timestamp(seconds: float) -> str:
    """
    Convert seconds into a VTT timestamp.

    :param seconds: The number of seconds, which may include values greater than or equal to 60. However, it will ignore
                    more than 60 minutes worth of time.
    :return: A string timestamp in the form MM:SS.FFF or MM:S.FFF
    """
    minutes, seconds, milliseconds = divide_seconds(seconds)
    return f"{minutes:02}:{seconds}.{milliseconds:03}"


def divide_seconds(seconds: float) -> Tuple[int, int, int]:
    """
    Split up seconds into a tuple of minutes, seconds, and milliseconds.
    Postcondition: 0 <= minutes < 60, 0 <= seconds < 60, 0 <= milliseconds < 1000

    :param seconds: The number of seconds, which may include values greater than or equal to 60. However, it will ignore
                    more than 60 minutes worth of time.
    :return: The number of minutes, seconds, and milliseconds as a tuple
    """
    milliseconds = seconds * 1e+3
    minutes = seconds // 60
    return int(minutes % 60), int(seconds % 60), int(milliseconds % 1e+3)


def realign_alignment(orig_alignment: transcription.Transcription) -> transcription.Transcription:
    """
    Create a new Gentle ``Transcription`` object with punctuation included in the words and all failed cases resolved by
    adding substitute durations and timings.

    Note: Phones will not be resolved.

    :param orig_alignment: An alignment produced by Gentle's transcriber that may contain failed cases
    :return: A copy of the alignment with punctuation included in the words and with inferred word timings based on
             their length
    """
    # Ref for char_speak_rate: https://github.com/achen4290/ForcedAligner/blob/master/flask_app/app.py#L80
    char_speak_rate = 0.075  # 75ms per character.
    realigned_words = []
    transcript = orig_alignment.transcript
    prev_end_time = 0
    prev_end_time_unaligned = 0
    total_lag = 0
    for aligned_word in orig_alignment.words:
        # Re-align words based on an approximation / fix any illogical timestamps.
        if not aligned_word.success():
            # The estimated amount of time it would take to say the word
            sub_spk_time = char_speak_rate * len(aligned_word.word)
            new_start = prev_end_time
            new_end = new_start + sub_spk_time
            new_duration = sub_spk_time
            prev_end_time_unaligned = new_end
        else:
            # Adjust for any previous instances of lag.
            new_start = aligned_word.start + total_lag
            new_end = aligned_word.end + total_lag
            new_duration = aligned_word.duration

            # Adjust for any instances where the start time comes before the end time of a previous word ("lag").
            if new_start < prev_end_time_unaligned:
                lag = prev_end_time_unaligned - new_start
                total_lag += lag
                new_start += lag
                new_end += lag

        new_start_offset = aligned_word.startOffset
        new_end_offset = aligned_word.endOffset

        # Adjust offsets to include punctuation.
        # The logic here is built this way to ensure that no whitespace is present in the slice.
        while new_start_offset > 0 and transcript[new_start_offset - 1] not in string.whitespace:
            new_start_offset -= 1
        while new_end_offset < len(transcript) and transcript[new_end_offset] not in string.whitespace:
            new_end_offset += 1

        # Update the "word" field to include punctuation.
        new_word = transcript[new_start_offset:new_end_offset]

        realigned_word = transcription.Word(transcription.Word.SUCCESS, new_start_offset, new_end_offset, new_word,
                                            aligned_word.alignedWord, aligned_word.phones,
                                            new_start, new_end, new_duration)
        realigned_words.append(realigned_word)
        prev_end_time = new_end

    # Second pass: Merge hyphenated words entered twice.
    prev_word = None
    for i, word in enumerate(realigned_words[:]):  # Use a temporary copy to avoid iteration issues w/ deleting items.
        if prev_word is not None and '-' in word.word and prev_word.word == word.word:
            word.start = prev_word.start
            word.duration += prev_word.duration
            realigned_words.pop(i - 1)
        prev_word = word

    return transcription.Transcription(transcript, realigned_words)
