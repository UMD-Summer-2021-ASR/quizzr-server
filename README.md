# Quizzr.io Back-end
## Overview
This is the code for the Quizzr server written in the Flask framework for Python 3.8. Features:
* Handles POST requests to the `/upload` webpage that include a form with the MIME type "multipart/form-data". The 
  form must include a WAV file under the field `audio`. Currently, it updates the MongoDB Quizzr Atlas in response to
  these requests and sends the upload to the Google Drive stored on the  Quizzr account.
* Handles GET requests to record and answer questions (through `/recordquestion/` and `/answerquestion/` respectively).
  It provides responses in JSON format.
* Handles GET requests for batches of unprocessed audio documents through `/audio/unprocessed/`.
* Handles POST requests for turning unprocessed audio documents into processed ones through `/audio/processed`.
## Installation
Prior to installation, you will need to have `pip` installed.
1. Clone this repository.
2. Install all the necessary dependencies by executing `pip install -r requirements.txt` in the folder of the repository.
   It may be a good idea to set up a virtual environment prior to doing this step to avoid conflicts with already
   installed packages.
3. Install [Gentle](https://github.com/lowerquality/gentle) by following the instructions in the README.md document. If
   you are installing it through the source code on a Linux operating system, you may need to change
   `install_deps.sh` to be based on your distribution.
4. Create the directories `privatedata` and `recordings` in the repository.
5. Create a text file named `connectionstring` in the `privatedata` directory and copy the connection string from the
   MongoDB Quizzr Atlas into this text file. Make sure that the name of the text file does not include any extensions, 
   such as `.txt`.
6. Login to the Quizzr Google Account on Google Cloud Platform and download the credentials file for the client "Quizzr 
   Server". Rename it to `gdrive_secret.json` and put it in the `privatedata` directory.
### Updating
To update the repository on your machine, either use `git pull` (requires you to commit your changes) or reinstall the
repository.
### Uninstalling
To uninstall this repository, simply delete its directory and the contents defining its associated virtual environment.
## Running the Server for Testing
The following instructions are for running the server for testing purposes. Please do not follow these instructions if
you plan on running it in production.

To start the server, enter the following commands into the terminal, replacing `dir` with the absolute directory of the
repository on your machine:
```bash
$ export FLASK_APP=server
$ flask run
```
If this is the first time running the server, you will be asked to go through an authentication process by navigating to
a URL. Please follow these instructions. \
You can view the website through http://127.0.0.1:5000/. \
Stop the server using Ctrl + C.

There is an option for running this server in debug mode. To do that, simply set `FLASK_ENV` to `development` in the
terminal. By default, the debugger is enabled. To disable the debugger, add `--no-debugger` to the run command.
### Testing
**Upload Handler:** Navigate to the page `/uploadtest/` and fill in the fields in the resulting GUI. You do not need to
fill in the question ID and user ID fields as of this version. Note that upon submitting, the contents of the atlas wil
be altered, and the server has no built-in way of reversing these changes. \
**Question Selectors:** Navigate to the page `/recordquestion/` for questions to record or the `/answerquestion/` for
questions to answer.
Unprocessed Audio Batch Request: Navigate to the page `/audio/unprocessed/`. \
**Processed Audio POST Request:** Navigate to the page `/processedaudiotest/` and fill in the fields in the resulting 
GUI. You will need to refer to the MongoDB Quizzr Atlas to get the Google Drive File ID.
