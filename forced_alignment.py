import logging
import gentle
import multiprocessing
'''
Created on Apr 30, 2021

@author: Christopher Rapp
Dependencies:
* gentle

Coded and tested in Python 3.7.10
'''

'''
Gentle is by Robert M Ochshorn and is licensed under the MIT License.

The MIT License (MIT)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in
all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
THE SOFTWARE.
'''


def get_forced_alignment(speech_file: str, transcript: str) -> gentle.Transcription:
    """
    Run a forced alignment using Gentle and return the result as a gentle.Transcription object

    :param speech_file: The path to the audio file
    :param transcript: The transcript of the speech in the audio file
    :return: The result as a gentle.Transcription object
    """
    def on_progress(p):  # align.py stuff
        for k, v in p.items():
            logging.debug("%s: %s" % (k, v))

    # Currently, this function uses Gentle and bases its code on a modified version of align.py, which is in Gentle's
    # source code.
    nthreads = multiprocessing.cpu_count()
    disfluency = False
    conservative = False
    disfluencies = {'uh', 'um'}

    logging.info("Retrieving forced alignment...")

    resources = gentle.Resources()

    logging.info("Converting audio to 8K sampled wav...")  # align.py step

    with gentle.resampled(speech_file) as wav_file:
        logging.info("Starting alignment...")  # align.py step
        aligner = gentle.ForcedAligner(resources, transcript, nthreads=nthreads, disfluency=disfluency,
                                       conservative=conservative, disfluences=disfluencies)
        result = aligner.transcribe(wav_file, progress_cb=on_progress, logging=logging)

    return result
