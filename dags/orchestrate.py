import time
from airflow.sdk import dag, task
from airflow.providers.standard.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState

# By default dbt only prints a warning and exits 0 when a --select matches nothing, so Airflow
# would show the task as green even though no model ran. This makes such a run fail instead.
FAIL_ON_EMPTY_SELECTION = '--warn-error-options \'{"error": ["NoNodesForSelectionCriteria", "NothingToDo"]}\''


@dag
def orchestrate():

    @task
    def ingest_cdc():
        # Reads DATABRICKS_HOST and DATABRICKS_TOKEN from the environment (set in airflow/.env)
        ws = WorkspaceClient()

        job_trigger = ws.jobs.run_now(job_id=108926061598438)

        while True:

            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break 
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
                    
            time.sleep(5)  # Wait for 5 seconds before checking the job status again
        
        return "CDC Ingestion Completed"
    
    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_project/target && rm -rf /opt/airflow/walmart_project/logs"

    @task.bash
    def source_freshness():
        # Manually set the working directory using the 'cd' command before running the dbt command
        return "cd /opt/airflow/walmart_project && dbt source freshness"
    

    silver_technical = BashOperator(
        task_id='silver_technical',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select Silver_t ' + FAIL_ON_EMPTY_SELECTION
    )

    silver_technical_tests = BashOperator(
        task_id='silver_technical_tests',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt test --select Silver_t ' + FAIL_ON_EMPTY_SELECTION
    )

    silver_business = BashOperator(
            task_id='silver_business',
            cwd='/opt/airflow/walmart_project',
            bash_command='dbt run --select silver_b ' + FAIL_ON_EMPTY_SELECTION
        )

    silver_business_tests = BashOperator(
        task_id='silver_business_tests',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt test --select silver_b ' + FAIL_ON_EMPTY_SELECTION
    )

    gold_dimensions = BashOperator(
        task_id='gold_dimensions',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt snapshot'
    )

    gold_facts = BashOperator(
        task_id='gold_facts',
        cwd='/opt/airflow/walmart_project',
        bash_command='dbt run --select fact_orders ' + FAIL_ON_EMPTY_SELECTION
    )

    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests  >> silver_business >> silver_business_tests >> gold_dimensions >> gold_facts

orchestrate_dag = orchestrate()