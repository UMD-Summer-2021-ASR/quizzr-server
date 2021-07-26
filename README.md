# Quizzr.io Data Flow Server

## Overview
The Quizzr.io data flow server functions as the central piece of the back-end, handling requests for changing and getting data in common but specific manners. Examples include pre-screening audio recordings of question transcripts, selecting a question to answer or record, and enabling asynchronous processing of audio. It is written in the Flask framework for Python 3.8 and uses Firebase Storage to store audio files and the MongoDB Quizzr Atlas to store data. It is especially designed to work with the front-end portion of Quizzr.io. See the [browser-asr](https://github.com/UMD-Summer-2021-ASR/browser-asr) repository for more details on how to set it up with the back-end.

## Installation
Prior to installation, you will need to have `pip` installed.
1. Clone this repository.
1. Install all the necessary dependencies by executing `pip install -r requirements.txt` in the folder of the repository. It may be a good idea to set up a virtual environment prior to doing this step to avoid conflicts with already installed packages.
1. Install [Gentle](https://github.com/lowerquality/gentle) by following the instructions in the associated README.md document. If you are installing it through the source code on a Linux operating system, you may need to change `install_deps.sh` to be based on your distribution. You will need to modify the `wget` command in `install_models.sh` to include the `--no-check-certificate` flag because the certificate for accessing `https://www.lowerquality.com` has expired.
1. Create a directory for the instance path of the server. By default, it is `~/quizzr_server`, but it can be overridden by the `Q_INST_PATH` environment variable or the `test_inst_path` parameter in the app factory function, `create_app`. In the instance path, create another directory called `secrets`.
1. Login to the Quizzr Google Account on Firebase and navigate to the Project settings --> Service accounts. Generate a private key for the Firebase Admin SDK service account and store it at `secrets/firebase_storage_key.json`.

### Updating
To update the repository on your machine, either use `git pull` (requires you to commit your changes) or reinstall the repository.

### Uninstalling
To uninstall this repository, simply delete its directory and the contents defining its associated virtual environment, along with the instance path.

## Running the Server
Prior to running the server, get the connection string for the MongoDB Client from the Quizzr Atlas (accessed through the Quizzr team account). \
To start the server, enter the following commands into the terminal, replacing `your-connection-string` with the connection string you obtained earlier:
```bash
$ export CONNECTION_STRING=your-connection-string
$ export FLASK_APP=server
$ flask run
```
You can view the website through http://127.0.0.1:5000/. \
Stop the server using Ctrl + C.

To run the server in debug mode, set `FLASK_ENV` to `development` in the terminal. By default, the debugger is enabled. To disable the debugger, add `--no-debugger` to the run command.
### Configuring the Server
Creating a JSON file named `sv_config.json` in the `config` subdirectory of the instance path allows for specifying a set of overrides to merge on top of the default configuration.
 All configuration fields must use purely capital letters to be recognized by the server. The following is a list of configuration fields and their descriptions:
* `UNPROC_FIND_LIMIT` The maximum number of unprocessed audio documents to find in a single batch.
* `DATABASE` The name of the database to use in MongoDB.
* `BLOB_ROOT` The name of the root folder to use in Firebase Storage.
* `BLOB_NAME_LENGTH` The length of the string to generate when naming uploaded audio files.
* `Q_ENV` The type of environment to use. It currently does not have much use outside of controlling access to certain pages.
* `SUBMISSION_FILE_TYPES` The file extensions to look for when deleting submissions.
* `DIFFICULTY_LIMITS` The upper bound of each difficulty, or `null` to have no upper bound.
* `VERSION` The version of the software. Used in audio document definitions for cases where the schema changes.
* `MIN_ANSWER_SIMILARITY` The program marks a given answer at the `/answer` `GET` endpoint as correct if the similarity between the answer and the correct answer exceeds this value.
* `PROC_CONFIG` Configuration for the recording processor. Includes:
  * `checkUnk` Check for unknown words along with unaligned words when calculating accuracy.
  * `unkToken` The value of the aligned word to look for when detecting out-of-vocabulary words.
  * `minAccuracy` The minimum acceptable accuracy of a submission.
  * `queueLimit` The maximum number of submissions to pre-screen at once.
* `VERIFY_AUTH_TOKENS` The program blocks access to certain resources for users not properly authenticated in Firebase if this field is `true`.

It is also possible to override configuration fields through environment variables or through a set of overrides passed into the `test_overrides` argument for the app factory function. Currently, overrides with environment variables only work with fields that have string values.

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

## Endpoints
All documentation for the endpoints has been moved to [api/backend.yaml](api/backend.yaml), which is in an OpenAPI format. You can view it with the [Swagger UI](https://swagger.io/tools/swagger-ui/) or a similar OpenAPI GUI generator.