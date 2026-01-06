#!/usr/bin/env python3
import dropbox
from dropbox import DropboxOAuth2FlowNoRedirect
import os
from dotenv import load_dotenv
load_dotenv()

# Use 'offline' to get a refresh token
auth_flow = DropboxOAuth2FlowNoRedirect(os.getenv("DROPBOX_APP_KEY"), os.getenv("DROPBOX_APP_SECRET"), token_access_type="offline")

authorize_url = auth_flow.start()
print(f"1. Go to: {authorize_url}")
print("2. Click 'Allow' (you might have to log in first).")
print("3. Copy the authorization code.")
auth_code = input("Enter the authorization code here: ").strip()

try:
    oauth_result = auth_flow.finish(auth_code)
    print(f"Successfully obtained refresh token: {oauth_result.refresh_token}")
    # Securely store this refresh token for future use
    # save_somewhere(oauth_result.refresh_token) 
except Exception as e:
    print(f'Error: {e}')
