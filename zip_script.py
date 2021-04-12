"""
Compresses files into a zip
These are files we need to run some Google Cloud Functions
"""

import zipfile

files = [
    "main.py",
    "db.py",
    "api.py",
    "./secrets/db_creds.json",
    "requirements.txt",
]

loczip = "crypto_project.zip"

zip = zipfile.ZipFile(loczip, "w")
for f in files:
    zip.write(f)
zip.close()
