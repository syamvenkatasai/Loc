"""
@author: Gopi Teja
Created Date: 2022-11-23
"""

import json
import schedule
import requests
import time
import os

from datetime import datetime, timedelta


# Write an function to hit the folder monitor flow
def start_folder_monitor(tenant_id, port, workflow_name):
    print(f"#### Started FM of {tenant_id}")
    host = os.environ.get('SERVER_IP')
    api_params = {}
    request_api = f'http://{host}:{port}/rest/engine/default/process-definition/key/{workflow_name}/start'
    headers = {'Content-type': 'application/json; charset=utf-8'}
    print(f"#### Hitting the camunda api of {request_api}")
    response = requests.post(request_api, json=api_params, headers=headers,verify=False)
    response_dict = json.loads(response.text)
    print(f"#### {tenant_id} FM Response Dict", response_dict)



def hit_get_files_from_sftp():
        print("Hitting get_files_from_sftp route...")
        host = 'foldermonitor'
        port = 443
        request_api = f'https://{host}:{port}/get_files_from_sftp'

        # Headers and payload
        headers = {'Content-type': 'application/json; charset=utf-8'}
        payload = {}  # Add any necessary data for the route if required

        try:
            response = requests.post(request_api, json=payload, headers=headers, verify=False)
            response_data = response.json()
            print(f"Response from get_files_from_sftp: {response_data}")
        except Exception as e:
            print(f"Error hitting get_files_from_sftp: {e}")


# Call that function
schedule.every(5).seconds.do(start_folder_monitor,
                              'hdfc','8080', 'hdfc_folder_monitor')

# This below line code is useful for hitting the camunda work flow for master data upload
#schedule.every(15).seconds.do(start_folder_monitor,'hdfc','8080', 'folder_monitor_sftp')



if __name__ == '__main__':
    while True:

        mode = os.environ.get('MODE')
        print("##########Mode is ",mode)
        if mode == "UAT":
            print("####In UAT ---> Hitting servers in the UAT only###")
            schedule.every(7).seconds.do(hit_get_files_from_sftp)
        else:
            print("######In DEV mode")

        schedule.run_pending()
        time.sleep(1)
