# if error is showing on import then it is just vscode which is a bit funky.
# code will run smoothly
from azure.datalake.store import core, lib
from azure.datalake.store.multithread import ADLDownloader
from os import listdir
from os.path import isfile, join
import os
import re
import json

directory_id = ''
application_key = ''
application_id = ''


adls_cred = lib.auth(tenant_id=directory_id,
                     client_secret=application_key, client_id=application_id)

adls_name = "dlsazewpmlitdatalake0011213"
source_name = "smds"
adls_dir = "datalake-prod/raw/smds/schema/initial/hql"

if not os.path.exists(source_name):
    os.mkdir(source_name)
    print("Directory", source_name, "created.")
else:
    print("Directory", source_name,
          "already exist, but will overwrite what is already available")

alds_client = core.AzureDLFileSystem(adls_cred, store_name=adls_name)
# print(alds_client.listdir(adls_dir))

# ADLS downloader api in python to download the file from server to local machine
# adls_dir - is the location on the server
# local dir - is the location on the local machine
# overwrite is set to True if the same shcemas are downloaded again in the folder
ADLDownloader(alds_client, adls_dir, source_name, overwrite=True)
files = [f for f in listdir(source_name) if isfile(join(source_name, f))]
print(type(files))
# reads through the list of files in the directory and deletes the files which is mentioned as bkp_
for item in files:
    if item.startswith("bkp_"):
        print("item %s" % item)
        os.remove(os.path.join(source_name, item))

p = re.compile('\(([^)]+)\)')
e = ""
with open("smds/batchruns_initial.hql") as fin:
    data = fin.read()
    ex = re.search(p, data).group()
    e = ex.replace('(', '')
    e = e.replace(')', '')
    e = e.split(',')

lines = [line.strip('\n') for line in e]
print(lines, type(lines), json.dumps(lines))
