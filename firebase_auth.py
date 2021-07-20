# Based on https://github.com/googleworkspace/python-samples/blob/master/drive/quickstart/quickstart.py. Copyright
# notice for the file below.
# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials


class FirebaseAuth:
    def __init__(self, token_dir):
        self.token_path = os.path.join(token_dir, 'token.json')
        # If modifying these scopes, delete the file token.json.
        scopes = ['https://www.googleapis.com/auth/drive']
        self.creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(self.token_path):
            self.creds = Credentials.from_authorized_user_file(self.token_path, scopes)
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    os.path.join(token_dir, 'gdrive_secret.json'), scopes)
                self.creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_path, 'w') as token:
                token.write(self.creds.to_json())

        self.service = build('drive', 'v3', credentials=self.creds)

    def refresh(self):
        self.creds.refresh(Request())
        with open(self.token_path, 'w') as token:
            token.write(self.creds.to_json())


if __name__ == '__main__':
    FirebaseAuth(os.path.join(os.path.dirname(__file__), "privatedata"))
