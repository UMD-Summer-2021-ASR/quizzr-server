# Quizzr.io Back-end

## Overview
This is the code for the back-end portion of the Quizzr server written in the Flask framework for Python 3.8. It is
designed to work with the front-end portion of the Quizzr server. See the 
[browser-asr](https://github.com/UMD-Summer-2021-ASR/browser-asr) repository for more details on how to set it up with
the back-end.
<!--TODO: Include more clear documentation on API endpoints-->
Features:
* Handles POST requests at the `/upload` webpage that include a form with the MIME type "multipart/form-data". The 
  form must include a WAV file under the field `audio`. Currently, it prescreens the submissions for accuracy using 
  forced alignments and updates the Google Drive stored on the Quizzr account and the MongoDB Quizzr Atlas to include 
  sufficiently accurate submissions.
* Handles GET requests to record and answer questions (at `/record/` and `/answer/` respectively).
  It provides responses in JSON format.
* Handles GET requests for batches of unprocessed audio documents through `/audio/unprocessed/`.
* Handles batch POST requests for turning unprocessed audio documents into processed ones at `/audio/processed`.
  Requires arguments to be in JSON format.
* Handles batch POST requests for uploading unrecorded questions at `/upload/question`. Requires arguments to be in JSON
  format.

## Installation
Prior to installation, you will need to have `pip` installed.
1. Clone this repository.
1. Install all the necessary dependencies by executing `pip install -r requirements.txt` in the folder of the repository.
   It may be a good idea to set up a virtual environment prior to doing this step to avoid conflicts with already
   installed packages.
1. Install [Gentle](https://github.com/lowerquality/gentle) by following the instructions in the README.md document. If
   you are installing it through the source code on a Linux operating system, you may need to change
   `install_deps.sh` to be based on your distribution.
1. Create the directories `privatedata` and `recordings` in the repository.
1. Login to the Quizzr Google Account on Google Cloud Platform and download the credentials file for the client "Quizzr 
   Server". Rename it to `gdrive_secret.json` and put it in the `privatedata` directory.

### Updating
To update the repository on your machine, either use `git pull` (requires you to commit your changes) or reinstall the
repository.

### Uninstalling
To uninstall this repository, simply delete its directory and the contents defining its associated virtual environment.

## Running the Server
Prior to running the server, get the connection string for the MongoDB Client from the Quizzr Atlas (accessed through
the Quizzr team account).\
To start the server, enter the following commands into the terminal, replacing `your-connection-string` with the
connection string you obtained earlier:
```bash
$ export CONNECTION_STRING=your-connection-string
$ export FLASK_APP=server
$ flask run
```
If this is the first time running the server, you will be asked to go through an authentication process by navigating to
a URL. Please follow these instructions.\
You can view the website through http://127.0.0.1:5000/. \
Stop the server using Ctrl + C.

There is an option for running this server in debug mode. To do that, simply set `FLASK_ENV` to `development` in the
terminal. By default, the debugger is enabled. To disable the debugger, add `--no-debugger` to the run command.

### Testing
**Upload Handler:** Navigate to the page `/uploadtest/` and fill in the fields in the resulting GUI. You do not need to
fill in the question ID and user ID fields as of this version. Note that upon submitting, the contents of the atlas wil
be altered, and the server has no built-in way of reversing these changes.\
**Question Selectors:** Navigate to the page `/recordquestion/` for questions to record or the `/answerquestion/` for
questions to answer.
Unprocessed Audio Batch Request: Navigate to the page `/audio/unprocessed/`.\
**Processed Audio POST Request:** ~~Navigate to the page `/processedaudiotest/` and fill in the fields in the resulting 
GUI. You will need to refer to the MongoDB Quizzr Atlas to get the Google Drive File ID.~~ This test has lost its
functionality due to changes in the accepted argument format.

## Using Docker
There is a Dockerfile that you can use to build the Docker image for this repository. Alternatively, you can pull from
the [Docker Hub repository](https://hub.docker.com/r/chrisrapp999/quizzr_server) for the image.\
Prior to starting the Docker container, you will need to do the following:
1. If you do not have `gdrive_authentication.py`, download it onto your machine.
1. Create a directory named `privatedata` in the same parent directory as `gdrive_authentication.py`.
1. Place `gdrive_secret.json` (see [Installation](#Installation)) in the `privatedata` directory.
1. Run `gdrive_authentication.py`.

The following command includes notable arguments for running this image:
  ```bash
  $ docker run -p 5000:5000 \
  -v <privatedata-volume>:/quizzr-src/privatedata \
  [-v <recordings-volume>:/quizzr-src/recordings] \
  -e CONNECTION_STRING=<your-connection-string> \
  <quizzr-server-image-name>
  ```
Notes:
* The volume mount at `/quizzr-src/recordings` is optional but is recommended. 
* `<privatedata-volume>` and `<recordings-volume>` are either named volumes or paths for bind mounts, and
  `<your-connection-string>` is the connection string for the MongoDB Client (see
  [Running the Server](#Running-the-Server)).
* You will need to have the `token.json` file in the location of the mounting point for
  `/quizzr-src/privatedata`.