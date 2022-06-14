import os
import json
import base64
import boto3
from lib.dotenv import load_dotenv
import lib.requests as requests



if __name__ == '__main__':
    load_dotenv()
    ACCESS_KEY = os.environ['AWS_ACCESS_KEY_ID']
    SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
    SESSION_TOKEN = os.environ['AWS_SESSION_TOKEN']
    mwaa_env_name = os.environ['YOUR_ENVIRONMENT_NAME']

    client = boto3.client(
        'mwaa',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY,
        aws_session_token=SESSION_TOKEN
    )

    mwaa_cli_token = client.create_cli_token(
        Name=mwaa_env_name
    )
    # print(f'MWAA_CLI_TOKEN: {mwaa_cli_token}')

    mwaa_auth_token = 'Bearer ' + mwaa_cli_token['CliToken']
    mwaa_webserver_hostname = 'https://{0}/aws_mwaa/cli'.format(mwaa_cli_token['WebServerHostname'])
    # ref: https://airflow.apache.org/docs/apache-airflow/2.2.2/cli-and-env-variables-ref.html#list_repeat2
    raw_data = "dags list -o json"

    mwaa_response = requests.post(
        mwaa_webserver_hostname,
        headers={
            'Authorization': mwaa_auth_token,
            'Content-Type': 'text/plain'
            },
        data=raw_data
        )
        
    mwaa_std_err_message = base64.b64decode(mwaa_response.json()['stderr']).decode('utf8')
    mwaa_std_out_message = base64.b64decode(mwaa_response.json()['stdout']).decode('utf8')

    print(f'Status Code : {mwaa_response.status_code}')
    print(f'Std_err : {mwaa_std_err_message}')
    print(f'Std_out : {mwaa_std_out_message}')