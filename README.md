#Bento Google Drive File Utility

## Prerequisites
- Python 3.6 or newer
- A Google Cloud project with Google Drive API enabled
- A Google service account for this application
- A service account API authentication key 

## Google Cloud Configuration
1. Create a project through the Google Cloud Console (console.cloud.google.com)
2. On the "APIs and Services" dashboard page, enable the "Google Drive API" for your project
3. On the "APIs and Services" credentials page, create a new service account
4. On the service account details page, go to the keys tab and select add key
5. Click create new key
6. Select JSON then click create
7. The service account private key JSON file should start downloading
8. Rename this file "client_secrets.json" and place it in the "auth" directory of this project

Note: Each service account private key can only be downloaded once, a new key will need to be generated each time this file needs to be replaced. Be sure to delete unused service account private keys from the Google Cloud Console when they are no longer in use.

## Command Line Arguments
- Google Drive Folder IDs
  - The Google Drive IDs of the folders for which the utility will generate and inventory report
  - A Google Drive folder URL is also valid, the utility will extract the Google Drive folder ID from the URL automatically
  - Command: ```-i/--google-id <Google Drive ID or Google Drive folder URL>```
  - Required
- Output Directory
  - An output directory in which the file inventory report will be generated, this directory must already exist
  - Command: ```-o/--output-dir <directory path>```
  - Not Required
  - Default Value: ```<The directory in which the utility is running>```

