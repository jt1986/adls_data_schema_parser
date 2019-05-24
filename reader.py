from azure.datalake.store import core, lib

directory_id = ''
application_key = ''
application_id = ''


adls_cred = lib.auth(tenant_id=directory_id,
                     client_secret=application_key, client_id=application_id)

adls_name = 'dlsazewpmlitdatalake001'
alds_client = core.AzureDLFileSystem(adls_cred, store_name=adls_name)

print(alds_client.listdir('datalake-prod/raw/smds'))
