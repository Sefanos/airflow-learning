from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

default_args = {
    'owner': 'Sefanoswhat',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'data_processing_zeta',
    default_args=default_args,
    description='Data processing workflow',
    schedule_interval='0 0 * * *',  # Run at midnight every day (cron expression)
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['data', 'processing'],
) as dag:
    
    # Start task
    start = EmptyOperator(
        task_id='start'
    )
    
    # Download data task
    download_data = BashOperator(
        task_id='download_data',
        bash_command='echo "Downloading data from source..." && sleep 3 && echo "Data downloaded successfully."',
    )
    
    # Process data function
    def process_data(**kwargs):
        print("Processing the downloaded data...")
        # This is where you would add your data processing logic
        # You can access task instance, DAG, etc. through the kwargs
        task_instance = kwargs['ti']
        # Example of using XCom to push data for other tasks to use
        task_instance.xcom_push(key='processed_data_rows', value=100)
        return "Data processing completed"
    
    # Process data task
    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
    )
    
    # Generate report function
    def generate_report(**kwargs):
        ti = kwargs['ti']
        # Example of using XCom to pull data from another task
        rows_processed = ti.xcom_pull(task_ids='process_data', key='processed_data_rows')
        print(f"Generating report for {rows_processed} processed rows...")
        return f"Report generated for {rows_processed} rows"
    
    # Generate report task
    generate_report_task = PythonOperator(
        task_id='generate_report',
        python_callable=generate_report,
        provide_context=True,
    )
    
    # Send report task
    send_report = BashOperator(
        task_id='send_report',
        bash_command='echo "Sending report to stakeholders..." && sleep 2 && echo "Report sent!"',
    )
    
    # End task
    end = EmptyOperator(
        task_id='end'
    )
    
    # Define the task dependencies
    start >> download_data >> process_data_task >> generate_report_task >> send_report >> end