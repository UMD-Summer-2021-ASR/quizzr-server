# Quizzr.io Data Flow Server

## Overview
The Quizzr.io data flow server functions as the central piece of the back-end, handling requests for changing and getting data in common but specific manners. Examples include pre-screening audio recordings of question transcripts, selecting a question to answer or record, and enabling asynchronous processing of audio. It is written in the Flask framework for Python 3.8 and uses Firebase Storage to store audio files and the MongoDB Quizzr Atlas to store data. It is especially designed to work with the front-end portion of Quizzr.io. See the [browser-asr](https://github.com/UMD-Summer-2021-ASR/browser-asr) repository for more details on how to set it up with the back-end.

## Prerequisites
Prior to installing the server, either through Docker (see [Using Docker](#Using-Docker)) or directly onto your machine, make sure you have addressed the following prerequisites:
* Python 3.8 with the latest version of `pip` installed (does not apply with Docker).
* A MongoDB Atlas with the following (see [Get Started with Atlas](https://docs.atlas.mongodb.com/getting-started/), parts 1-5, for more information):
  * An Atlas account; 
  * A cluster on version 4.4.x with a database that contains the "UnprocessedAudio", "Audio", "RecordedQuestions", "UnrecordedQuestions", "Games", and "Users" collections, all of which are not capped;
  * A database user with permission to read and write to all collections in the database; and
  * A connection through the database user with a Python driver version 3.6 or later.
* A Firebase project with the Cloud Storage service enabled and a connection to the project through an Admin SDK set up (see [Cloud Storage for Firebase](https://firebase.google.com/docs/storage/#implementation_path) and [Add the Firebase Admin SDK to your server](https://firebase.google.com/docs/admin/setup) respectively for more information).

## Installation
1. Clone this repository.
1. Install all the necessary dependencies by executing `pip install -r requirements.txt` in the folder of the repository. It may be a good idea to set up a virtual environment prior to doing this step to avoid conflicts with already installed packages.
1. Install [Gentle](https://github.com/lowerquality/gentle) by following the instructions in the associated README.md document. If you are installing it through the source code on a Linux operating system, you may need to change `install_deps.sh` to be based on your distribution.
1. Create a directory for the instance path of the server. By default, it is `~/quizzr_server`, but it can be overridden by the `Q_INST_PATH` environment variable or the `test_inst_path` parameter in the app factory function, `create_app`. In the instance path, create another directory called `secrets`.
1. Generate a private key for the Firebase Admin SDK service account and store it at `secrets/firebase_storage_key.json`.

### Updating
To update the repository on your machine, either use `git pull` (requires you to commit your changes) or reinstall the repository.

### Uninstalling
To uninstall this repository, simply delete its directory and the contents defining its associated virtual environment, along with the instance path.

## Configuring the Server
Creating a JSON file named `sv_config.json` in the `config` subdirectory of the instance path allows for specifying a set of overrides to merge on top of the default configuration.
 All configuration fields must use purely capital letters to be recognized by the server. The following is a list of configuration fields and their descriptions:
* `UNPROC_FIND_LIMIT` The maximum number of unprocessed audio documents to find in a single batch.
* `DATABASE` The name of the database to use in MongoDB.
* `BLOB_ROOT` The name of the root folder to use in Firebase Storage.
* `Q_ENV` The type of environment to use. A value of `development` or `testing` makes the server identify unauthenticated users as `dev` and allows access to the `/uploadtest` endpoint.
* `SUBMISSION_FILE_TYPES` The file extensions to look for when deleting submissions.
* `DIFFICULTY_DIST` The fractional distribution of recordings by difficulty. Example: `[0.6, 0.3, 0.1]` makes the 60% least difficult recordings have a "0" difficulty, followed by the next 30% at difficulty "1", and the rest at difficulty "2".
* `VERSION` The version of the software. Used in audio document definitions for cases where the schema changes.
* `MIN_ANSWER_SIMILARITY` The program marks a given answer at the `/answer` `GET` endpoint as correct if the similarity between the answer and the correct answer exceeds this value.
* `PROC_CONFIG` Configuration for the recording processor. Includes:
  * `checkUnk` Check for unknown words along with unaligned words when calculating accuracy.
  * `unkToken` The value of the aligned word to look for when detecting out-of-vocabulary words.
  * `minAccuracy` The minimum acceptable accuracy of a submission.
  * `queueLimit` The maximum number of submissions to pre-screen at once.
* `DEV_UID` The default user ID of an unauthenticated user in a `development` environment.
* `LOG_PRIVATE_DATA` Redact sensitive information, such as Firebase ID tokens, in log messages.
* `VISIBILITY_CONFIGS` A set of configurations that determine which collection to retrieve a profile from and what projection to apply. Projections are objects with the key being the field name and the value being 1 or 0, representing whether to include or exclude the field.
* `USE_ID_TOKENS` A configuration option specifying whether to use a Firebase ID token or to use the raw contents for identifying the user. Has no effect when `TESTING` is False.
* `MAX_LEADERBOARD_SIZE` The maximum number of entries allowable on the leaderboard.
* `DEFAULT_LEADERBOARD_SIZE` The default number of entries on the leaderboard.
* `MAX_USERNAME_LENGTH` The maximum allowable number of characters in a username.
* `USERNAME_CHAR_SET` A string containing all allowable characters in a username.
* `DEFAULT_RATE_LIMITS` An array containing request rate limits (in a string format) for all server endpoints. Examples: "200 per day", "50 per hour", "1/second"

It is also possible to override configuration fields through environment variables or through a set of overrides passed into the `test_overrides` argument for the app factory function. Currently, overrides with environment variables only work with fields that have string values.

### Configuration Defaults
The following JSON data shows the default values of each configuration field. You may also view the default configuration in `server.py`.
```json
{
  "UNPROC_FIND_LIMIT": 32,
  "DATABASE": "QuizzrDatabase",
  "BLOB_ROOT": "production",
  "Q_ENV": "production",
  "SUBMISSION_FILE_TYPES": ["wav", "json", "vtt"],
  "DIFFICULTY_DIST": [0.6, 0.3, 0.1],
  "VERSION": "0.2.0",
  "MIN_ANSWER_SIMILARITY": 50,
  "PROC_CONFIG": {
    "checkUnk": true,
    "unkToken": "<unk>",
    "minAccuracy": 0.5,
    "queueLimit": 32
  },
  "DEV_UID": "dev",
  "LOG_PRIVATE_DATA": false,
  "VISIBILITY_CONFIGS": {
    "basic": {
      "projection": {"pfp": 1, "username": 1, "usernameSpecs": 1},
      "collection": "Users"
    },
    "public": {
      "projection": {"pfp": 1, "username": 1, "usernameSpecs": 1},
      "collection": "Users"
    },
    "private": {
      "projection": null,
      "collection": "Users"
    }
  },
  "USE_ID_TOKENS": true,
  "MAX_LEADERBOARD_SIZE": 200,
  "DEFAULT_LEADERBOARD_SIZE": 10,
  "MAX_USERNAME_LENGTH": 16,
  "USERNAME_CHAR_SET": "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789",
  "DEFAULT_RATE_LIMITS": []
}
```

## Running the Server
Prior to running the server, get the connection string for the MongoDB Client from the Quizzr Atlas (accessed through the Quizzr Google account). \
To start the server, enter the following commands into the terminal, replacing `your-connection-string` with the connection string you obtained earlier:
```bash
$ export CONNECTION_STRING=your-connection-string
$ export FLASK_APP=server
$ flask run
```
Alternatively, you can run the server.py module (see `python 3 server.py -h` for more information):
```bash
$ export CONNECTION_STRING=your-connection-string
$ python3 server.py
```
You can view the website through http://127.0.0.1:5000/. \
Stop the server using Ctrl + C.

To run the server in debug mode, set `FLASK_ENV` to `development` in the terminal. By default, the debugger is enabled. To disable the debugger, add `--no-debugger` to the run command.

### Testing
There is a separate repository for running automated tests on the server. See the [quizzr-server-test](https://github.com/UMD-Summer-2021-ASR/quizzr-server-test) repository for more information.

## Using Docker
There is a Dockerfile that you can use to build the Docker image for this repository. Alternatively, you can pull from the [Docker Hub repository](https://hub.docker.com/r/chrisrapp999/quizzr_server) for the image.

The following command includes notable arguments for running this image:
  ```bash
  $ docker run -p 5000:5000 \
  -v <config-volume>:/root/quizzr_server/config \
  -v <secrets-volume>:/root/quizzr_server/secrets \
  -v <storage-volume>:/root/quizzr_server/storage \
  -e CONNECTION_STRING=<your-connection-string> \
  <quizzr-server-image-name>
  ```
Notes:
* Each volume specified is either a named volume or a path for a bind mount, and `<your-connection-string>` is the connection string for the MongoDB Client (see [Running the Server](#Running-the-Server)).
* You will need to have the private Firebase key in the mounting directory (see [Installation](#Installation)).

## Troubleshooting
The following contains potential problems you may encounter while installing or running this server:
* The installation execution for Gentle fails due to the certificate expiring: Modify the `wget` command in `install_models.sh` to include the `--no-check-certificate` flag.
* The execution for installing Gentle fails to create `kaldi.mk`: Run `./configure --enable-static`, `make`, and `make install` in `gentle/ext/kaldi/tools/openfst`. Then, run `install.sh` again.

## Batch UUID Issues
A recent update has added the requirement for a `batchUUID` field in segmented audio documents. A script has been added in the [maintenance](maintenance) folder to retroactively add this field to old audio documents.

## Endpoints
All documentation for the endpoints has been moved to [reference/backend.yaml](reference/backend.yaml), which is in an OpenAPI format. You can view it with the [Swagger UI](https://swagger.io/tools/swagger-ui/) or a similar OpenAPI GUI generator.
