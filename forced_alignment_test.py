import threading
import time
import json
from playsound import playsound

from forced_alignment import get_forced_alignment

'''
Created on Apr 30, 2021

@author: Christopher Rapp
Dependencies:
* gentle
* playsound
* forced_alignment.py

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


def play_function(speech_file):
    playsound(speech_file)


def _test_forced_alignment(speech_file_dir, alignment):
    t = threading.Thread(target=play_function, args=(speech_file_dir,))
    t.start()

    words = alignment["words"]
    now = 0.0

    for word_data in words:
        if word_data["case"] == 'success':
            time.sleep(word_data["start"] - now)
            print(word_data["word"], end=' ', flush=True)
            now = word_data["start"]
        elif word_data["case"] == 'not-found-in-audio':
            print(word_data["word"], end='** ', flush=True)

    t.join()
    print()


def main():
    speech_file_dir = input("Please enter the directory of the speech you want to use(*.wav, *.mp3, *.ogg, etc.): ")
    transcript_dir = input("Please enter the associated transcript directory: ")

    logging_level = input("Please enter the logging level (DEBUG, INFO, WARNING, ERROR, or CRITICAL, default INFO): ")
    if logging_level == '':
        logging_level = 'INFO'

    with open(transcript_dir, encoding='utf-8') as f:
        transcript = f.read()

    alignment = get_forced_alignment(speech_file_dir, transcript, log_level=logging_level)
    _test_forced_alignment(speech_file_dir, alignment)


def main2():
    speech_file_dir = input("Please enter the directory of the speech you want to use(*.wav, *.mp3, *.ogg, etc.): ")
    alignment_dir = input("Please enter the alignment directory: ")

    with open(alignment_dir) as f:
        alignment = json.load(f)

    _test_forced_alignment(speech_file_dir, alignment)


if __name__ == '__main__':
    OPTION_IN = "Do you want to load a pre-existing alignment?(y/n): "
    option = input(OPTION_IN).lower()
    while option != 'y' and option != 'n':
        print("Invalid option. Choose y or n.")
        option = input(OPTION_IN).lower()

    if option == 'y':
        main2()
    elif option == 'n':
        main()
