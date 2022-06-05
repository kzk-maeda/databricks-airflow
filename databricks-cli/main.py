from http import client
import os

import lib.databricks_cli.sdk.service as services
from lib.databricks_cli.sdk import ApiClient, JobsService
from lib.dotenv import load_dotenv

RUN_JOB = False

class Databricks:
    def __init__(self, host, token):
        """
        host, clientを渡してコンストラクタで初期化
        汎用paramsは一旦無視
        """
        if not host.startswith("https://"):
            host = "https://" + host
        self.client = ApiClient(
            host=host,
            token=token
        )
    
    def job(self):
        return JobsService(client=self.client)


if __name__ == "__main__":
    load_dotenv()
    host = os.environ['DATABRICKS_HOST']
    token = os.environ['DATABRICKS_TOKEN']

    print(host, token)

    # init api client
    client = Databricks(host=host, token=token)

    # init jobs
    job = client.job()

    # get job list
    jobs = job.list_jobs()
    print(jobs)

    # run job currently
    job_id = os.environ['DATABRICKS_JOB_ID']
    if RUN_JOB:
        res = job.run_now(job_id=job_id)
        print(res)

    # get running jobs
    running_jobs = job.list_runs()
    print(running_jobs)

    # get job output
    output = job.get_run_output(run_id=13140)
    print(output)