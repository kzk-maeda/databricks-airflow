import os
from lib.databricks_api import DatabricksAPI
from lib.dotenv import load_dotenv

RUN_JOB = False

if __name__ == "__main__":
  load_dotenv()
  host = os.environ['DATABRICKS_HOST']
  token = os.environ['DATABRICKS_TOKEN']

  print(host, token)
  db = DatabricksAPI(
    host=host,
    token=token
  )

  # get job list
  jobs = db.jobs.list_jobs()
  print(jobs.get('jobs'))

  # run job currently
  job_id = os.environ['DATABRICKS_JOB_ID']
  if RUN_JOB:
    res = db.jobs.run_now(
      job_id=job_id
    )
    print(res)

  # get running jobs
  running_jobs = db.jobs.list_runs()
  print(running_jobs)

  # get job output
  output = db.jobs.get_run_output(
    run_id=4651
  )
  print(output)