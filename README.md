# Description
This is the code for the Quizzr server written in the Flask framework for Python 3.8. It handles POST requests to the
`/upload/` webpage that include a form with the MIME type "multipart/form-data". The form must include the string fields
`questionId` and `userId` and a file under the field `audio`. Currently, it only updates the MongoDB Quizzr Atlas in
response to these requests and does not store the upload at all.
# Installation
1. Clone this repository.
2. Install `pip`.
3. Install all the necessary dependencies by executing `pip install requirements.txt` in the folder of the repository.
4. Get the connection string from the MongoDB Quizzr Atlas and copy it into a text file.
5. Set the environment variable `CONNECTION_STRING_PATH` to be the absolute path of the text file on your machine by
   executing `export CONNECTION_STRING_PATH=<path>`, replacing `<path>` with this path.
# Running the Server for Testing
The following instructions are for running the server for testing purposes. Please do not follow these instructions if
you plan on running it in production.

To start the server, enter the following commands into the terminal:
```bash
$ export FLASK_APP=server
$ flask run
```
You can view the website through http://127.0.0.1:5000/. \
Stop the server using Ctrl + C.

There is an option for running this server in debug mode. To do that, simply set `FLASK_ENV` to `development` in the
terminal. By default, the debugger is enabled. To disable the debugger, add `--no-debugger` to the run command.
## Testing the Upload Handler
To test the upload handler component of the server, navigate to the page `/uploadtest/` and fill in the fields in the
resulting GUI. You will need to refer to the MongoDB Quizzr Atlas to fill in the Question ID and User ID fields. Note
that upon submitting, the contents of the atlas will be altered, and the server has no built-in way of reversing these 
changes.