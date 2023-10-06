import os
import json
import asyncio
from typing import Dict, List

AZCOPY_PATH = os.getenv('AZCOPY_PATH')

# sample_storage_account_sftp_backup = {
#     'name': 'iisdpprodccstacc1',
#     'container': 'prime-reporting-prod',
#     'token': 'sp=rawl&st=2023-09-21T18:59:07Z&se=2027-01-02T03:59:07Z&spr=https&sv=2022-11-02&sr=c&sig=3U1SwhsZOY8E1K2Rs%2FK%2FSKgRrBRtu%2FvuZ6kdJx6314k%3D',
#     'folder': ''
# }

from_storage_account_sftp_backup = {
    'name': 'nbinimportdataarchive',
    'container': 'sftp-backup',
    'token': 'sv=2021-10-04&se=2023-10-27T16%3A07%3A32Z&sr=c&sp=rdl&sig=qiPdizTmtkfTyRAQ5ZdDwYZqzTKKTNJE715ostTpU68%3D',
    'folder': '',
    'file_path': 'C:/Repos/iis/tool/from_storage_account_sftp_backup.txt'
}

from_storage_account_dbbackup = {
    'name': 'nbinimportdataarchive',
    'container': 'dbbackup',
    'token': 'sv=2021-10-04&se=2023-10-26T20%3A03%3A26Z&sr=c&sp=rdl&sig=Q9zWdFgZjFTP3xGwGTDuAaxDcZje%2FLTUyOsx4GY%2FY8g%3D',
    'folder': '',
    'file_path': 'C:/Repos/iis/tool/from_storage_account_dbbackup.txt'
}

to_storage_account = {
    'name': 'iisdpprodccstacc1',
    'container': 'etl-backup-prod',
    'token': 'sv=2021-10-04&se=2023-10-26T19%3A44%3A41Z&sr=c&sp=rwl&sig=7DcNc0d9RfJxCn3e51KKLPZm1e5w0an7vzdPagrr5uQ%3D',
    'folder': 'SIS',
    'file_path': 'C:/Repos/iis/tool/to_storage_account.txt'
}

def _get_env_vars_for_azcopy() -> Dict[str, str]:
    current_envs = os.environ.copy()
    current_envs["AZCOPY_CONCURRENCY_VALUE"] = "AUTO"
    current_envs["AZCOPY_CRED_TYPE"] = "Anonymous"
    return current_envs

async def _list_files(storage_account: Dict[str, str]):    
    blob_url = f"https://{storage_account['name']}.blob.core.windows.net/{storage_account['container']}/{storage_account['folder']}?{storage_account['token']}"
    command = f'''{AZCOPY_PATH} list "{blob_url}" --output-type=json'''
    proc = await asyncio.create_subprocess_shell(command,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE,
                                                env=_get_env_vars_for_azcopy())
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise Exception(f'''Error in calling the azcopy command. Stdout: {stdout.decode() if stdout else ""} - Stderr: {stderr.decode() if stderr else ""}''')

    json = _convert_output_to_json(stdout.decode())
    return json

def _build_full_blob_url(storage_account: Dict[str, str], path: str):
    blob_url = f"https://{storage_account['name']}.blob.core.windows.net/{storage_account['container']}"
    if storage_account['folder']:
        blob_url = blob_url + '/' + storage_account['folder']
    if path:
        blob_url = blob_url + '/' + path
    blob_url = blob_url + '?' + storage_account['token']
    return blob_url

async def _copy_file(
        from_storage_account: Dict[str, str], 
        to_storage_account: Dict[str, str], 
        from_path: str,
        to_path: str):
    from_blob_url = _build_full_blob_url(from_storage_account, from_path) 
    to_blob_url = _build_full_blob_url(to_storage_account, to_path)
    command = f'''{AZCOPY_PATH} copy "{from_blob_url}" "{to_blob_url}" --overwrite=false --from-to=BlobBlob --s2s-preserve-access-tier=false --check-length=true --include-directory-stub=false --s2s-preserve-blob-tags=false --recursive --log-level=INFO'''
    proc = await asyncio.create_subprocess_shell(command,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE,
                                                env=_get_env_vars_for_azcopy())
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise Exception(f'''Error in calling the azcopy command. Stdout: {stdout.decode() if stdout else ""} - Stderr: {stderr.decode() if stderr else ""}''')
    return from_blob_url

async def _delete_file(full_url: str):
    command = f'''{AZCOPY_PATH} remove "{full_url}" --from-to=BlobTrash --recursive --log-level=INFO'''
    proc = await asyncio.create_subprocess_shell(command,
                                                stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE,
                                                env=_get_env_vars_for_azcopy())
    stdout, stderr = await proc.communicate()
    if proc.returncode != 0:
        raise Exception(f'''Error in calling the azcopy command. Stdout: {stdout.decode() if stdout else ""} - Stderr: {stderr.decode() if stderr else ""}''')

def _convert_output_to_json(input: str):
    decoded_data_as_json_string = input.replace("}\n{", "},{")
    decoded_data_as_json_object = json.loads(f"[{decoded_data_as_json_string}]")
    return decoded_data_as_json_object

def _clean_file_name(input: str):
    return input.strip().replace('\t', '').replace('\n', '')

async def _red_files_from_storage_account(storage_account: Dict[str, str]):
    files = []
    file_data_json = await _list_files(storage_account)
    for item in file_data_json:
        content = item["MessageContent"]
        if (content):
            info = content.split(";")[0]
            file_name = info.split(":")[1]
            files.append(file_name)
    return files

async def _populate_files_from_storage_account():
    from_storage_account_sftp_backup_files = await _red_files_from_storage_account(from_storage_account_sftp_backup)
    with open(from_storage_account_sftp_backup['file_path'], 'a') as the_file:
        for file_name in from_storage_account_sftp_backup_files:            
            the_file.write(f'{file_name}\n')
            print(file_name)

    from_storage_account_dbbackup_files = await _red_files_from_storage_account(from_storage_account_dbbackup)
    with open(from_storage_account_dbbackup['file_path'], 'a') as the_file:
        for file_name in from_storage_account_dbbackup_files:            
            the_file.write(f'{file_name}\n')
            print(file_name)            

    to_storage_account_files = await _red_files_from_storage_account(to_storage_account)
    with open(to_storage_account['file_path'], 'a') as the_file:
        for file_name in to_storage_account_files:            
            the_file.write(f'{file_name}\n')
            print(file_name)

async def _read_file_names_from_file(file_path):
    files = []
    with open(file_path) as file:
        for line in file:
            files.append(_clean_file_name(line.rstrip()))
    return files

async def run():

    #_populate_files_from_storage_account()

    #await _read_file_names_from_file('C:/Repos/iis/tool/from_storage_account_sftp_backup.txt')
    #await _read_file_names_from_file('C:/Repos/iis/tool/to_storage_account.txt')
    
    # from_storage_account_dbbackup_files = await _red_files_from_storage_account(from_storage_account_dbbackup)
    # from_file_file_names = await _read_file_names_from_file(from_storage_account_dbbackup['file_path'])
    
    # for from_storage_account_dbbackup_file in from_storage_account_dbbackup_files:
    #     from_storage_account_dbbackup_file = _clean_file_name(from_storage_account_dbbackup_file)
    #     if _contains(from_file_file_names, from_storage_account_dbbackup_file):
    #         folder = from_storage_account_dbbackup_file[:6]           
    #         moved_file_full_url = await _copy_file(from_storage_account_dbbackup, to_storage_account, from_storage_account_dbbackup_file, f'{folder}/{from_storage_account_dbbackup_file}')
    #         print(f'Moved File: {from_storage_account_dbbackup_file}')
    #         await _delete_file(moved_file_full_url)
    #         print(f'Deleted Source File: {from_storage_account_dbbackup_file}')

    from_storage_account_sftp_backup_files = await _red_files_from_storage_account(from_storage_account_sftp_backup)
    from_file_file_names = await _read_file_names_from_file(from_storage_account_sftp_backup['file_path'])
    
    total_storage_account_length = len(from_storage_account_sftp_backup_files)
    total_file_length = len(from_file_file_names)
    counter = 1

    print(f'Files to Move: {total_storage_account_length}')

    for from_storage_account_sftp_backup_file in from_storage_account_sftp_backup_files:
        from_storage_account_sftp_backup_file = _clean_file_name(from_storage_account_sftp_backup_file)
        if _contains(from_file_file_names, from_storage_account_sftp_backup_file):
            folder = from_storage_account_sftp_backup_file[:6]           
            moved_file_full_url = await _copy_file(from_storage_account_sftp_backup, to_storage_account, from_storage_account_sftp_backup_file, f'{folder}/{from_storage_account_sftp_backup_file}')
            print(f'Moved File ({counter}): {from_storage_account_sftp_backup_file}')
            await _delete_file(moved_file_full_url)
            print(f'Deleted Source File: {from_storage_account_sftp_backup_file}')
            counter = counter + 1

def _contains(list: List, value: str):
    for item in list:
        if _clean_file_name(item) == _clean_file_name(value):
            return True
    return False
    
if __name__ == '__main__':
     asyncio.run(run())
