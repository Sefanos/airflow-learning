import os
import pandas as pd
from datetime import datetime, timedelta
import requests
from io import StringIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

# Define default arguments
default_args = {
    'owner': 'Sefanosok',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Define the data directory
DATA_DIR = '/opt/airflow/data'
RAW_DATA_DIR = f'{DATA_DIR}/raw'
PROCESSED_DATA_DIR = f'{DATA_DIR}/processed'

# Define the source URL
CSV_URL = 'https://drive.google.com/uc?id=1NW7EnwxuY6RpMIxOazRVibOYrZfMjsb2&export=download'

# Create a DAG instance
with DAG(
    'csv_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for CSV data from Google Drive',
    schedule_interval='0 6 * * *',  # Run daily at 6 AM
    start_date=datetime(2025, 5, 27),
    catchup=False,
    tags=['data', 'etl', 'csv'],
) as dag:
    
    # Task to create directories if they don't exist
    create_directories = BashOperator(
        task_id='create_directories',
        bash_command=f'mkdir -p {RAW_DATA_DIR} {PROCESSED_DATA_DIR}',
    )
    
    # Define function to download the CSV file
    def download_csv_file(**kwargs):
        """
        Downloads a CSV file from the specified URL and saves it locally
        """
        # Create a timestamp for the filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"{RAW_DATA_DIR}/data_{timestamp}.csv"
        
        try:
            # Download the file
            response = requests.get(CSV_URL)
            response.raise_for_status()  # Raise exception for HTTP errors
            
            # Save the file locally
            with open(output_file, 'w') as f:
                f.write(response.text)
            
            print(f"CSV file successfully downloaded to {output_file}")
            
            # Push the file path to XCom for the next task
            kwargs['ti'].xcom_push(key='csv_file_path', value=output_file)
            return output_file
        except Exception as e:
            print(f"Error downloading CSV file: {e}")
            raise
    
    # Task to download the CSV file
    download_csv = PythonOperator(
        task_id='download_csv',
        python_callable=download_csv_file,
        provide_context=True,
    )
    
    # Define function to validate the CSV data
    def validate_csv_data(**kwargs):
        """
        Validates the downloaded CSV file for basic quality issues
        """
        # Get the CSV file path from XCom
        ti = kwargs['ti']
        csv_file_path = ti.xcom_pull(task_ids='download_csv', key='csv_file_path')
        
        try:
            # Load the CSV file into a pandas DataFrame
            df = pd.read_csv(csv_file_path)
            
            # Perform basic validation checks
            row_count = len(df)
            col_count = len(df.columns)
            missing_values = df.isna().sum().sum()
            
            print(f"CSV Validation Results:")
            print(f"- Rows: {row_count}")
            print(f"- Columns: {col_count}")
            print(f"- Missing values: {missing_values}")
            
            # Store validation results in XCom
            validation_results = {
                'row_count': row_count,
                'col_count': col_count,
                'missing_values': missing_values,
                'columns': df.columns.tolist()
            }
            
            ti.xcom_push(key='validation_results', value=validation_results)
            
            # Pass the original file path for the next task
            return csv_file_path
        except Exception as e:
            print(f"Error validating CSV data: {e}")
            raise
    
    # Task to validate the CSV data
    validate_csv = PythonOperator(
        task_id='validate_csv',
        python_callable=validate_csv_data,
        provide_context=True,
    )
    
    # Define function to transform the CSV data
    def transform_csv_data(**kwargs):
        """
        Transforms the CSV data and saves the processed file
        """
        # Get the CSV file path from XCom
        ti = kwargs['ti']
        csv_file_path = ti.xcom_pull(task_ids='validate_csv')
        validation_results = ti.xcom_pull(task_ids='validate_csv', key='validation_results')
        
        try:
            # Load the CSV file into a pandas DataFrame
            df = pd.read_csv(csv_file_path)
            original_shape = df.shape
            
            # Perform transformations (customize these based on your data):
            
            # 1. Drop duplicates
            df = df.drop_duplicates()
            
            # 2. Handle missing values (fill or drop)
            df = df.fillna(0)  # Replace with appropriate strategy for your data
            
            # 3. Convert data types if needed
            # Example: for column in numeric_columns: df[column] = pd.to_numeric(df[column], errors='coerce')
            
            # 4. Add derived columns (e.g., calculate new metrics)
            # Example: df['new_column'] = df['column1'] + df['column2']
            
            # 5. Rename columns if needed
            # Example: df = df.rename(columns={'old_name': 'new_name'})
            
            # Create timestamp for the processed filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            processed_file = f"{PROCESSED_DATA_DIR}/processed_data_{timestamp}.csv"
            
            # Save the processed DataFrame
            df.to_csv(processed_file, index=False)
            
            # Calculate transformation stats
            new_shape = df.shape
            transformation_stats = {
                'original_rows': original_shape[0],
                'original_cols': original_shape[1],
                'processed_rows': new_shape[0],
                'processed_cols': new_shape[1],
                'rows_removed': original_shape[0] - new_shape[0],
                'processed_file': processed_file
            }
            
            # Push the transformation stats to XCom
            ti.xcom_push(key='transformation_stats', value=transformation_stats)
            
            print(f"Transformation complete. Processed file saved to {processed_file}")
            print(f"Original shape: {original_shape}, New shape: {new_shape}")
            
            return processed_file
        except Exception as e:
            print(f"Error transforming CSV data: {e}")
            raise
    
    # Task to transform the CSV data
    transform_csv = PythonOperator(
        task_id='transform_csv',
        python_callable=transform_csv_data,
        provide_context=True,
    )
    
    # Define function to analyze the transformed data
    def analyze_data(**kwargs):
        """
        Performs basic analysis on the transformed data
        """
        # Get the processed file path from XCom
        ti = kwargs['ti']
        processed_file = ti.xcom_pull(task_ids='transform_csv')
        transformation_stats = ti.xcom_pull(task_ids='transform_csv', key='transformation_stats')
        
        try:
            # Load the processed CSV file
            df = pd.read_csv(processed_file)
            
            # Perform basic analysis
            summary_stats = df.describe().to_dict()
            column_types = df.dtypes.astype(str).to_dict()
            
            # Create an analysis report
            analysis_report = {
                'summary_stats': summary_stats,
                'column_types': column_types,
                'transformation_stats': transformation_stats
            }
            
            # Save the analysis report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = f"{PROCESSED_DATA_DIR}/analysis_report_{timestamp}.txt"
            
            with open(report_file, 'w') as f:
                f.write("=== DATA ANALYSIS REPORT ===\n\n")
                f.write(f"Report generated at: {datetime.now()}\n\n")
                
                f.write("=== TRANSFORMATION STATISTICS ===\n")
                f.write(f"Original rows: {transformation_stats['original_rows']}\n")
                f.write(f"Processed rows: {transformation_stats['processed_rows']}\n")
                f.write(f"Rows removed: {transformation_stats['rows_removed']}\n\n")
                
                f.write("=== COLUMN TYPES ===\n")
                for col, dtype in column_types.items():
                    f.write(f"{col}: {dtype}\n")
                f.write("\n")
                
                f.write("=== SUMMARY STATISTICS ===\n")
                for col, stats in summary_stats.items():
                    f.write(f"Column: {col}\n")
                    for stat, value in stats.items():
                        f.write(f"  - {stat}: {value}\n")
                    f.write("\n")
            
            print(f"Analysis report saved to {report_file}")
            return report_file
        except Exception as e:
            print(f"Error analyzing data: {e}")
            raise
    
    # Task to analyze the data
    analyze_data_task = PythonOperator(
        task_id='analyze_data',
        python_callable=analyze_data,
        provide_context=True,
    )
    
    # Define task for data quality check
    def check_data_quality(**kwargs):
        """
        Performs data quality checks on the processed data
        """
        ti = kwargs['ti']
        processed_file = ti.xcom_pull(task_ids='transform_csv')
        
        try:
            # Load the processed CSV file
            df = pd.read_csv(processed_file)
            
            # Define quality checks (customize these based on your data)
            quality_checks = {
                'no_missing_values': df.isna().sum().sum() == 0,
                'no_duplicates': df.duplicated().sum() == 0,
                'row_count_check': len(df) > 0
                # Add more checks as needed
            }
            
            # Check if all quality checks passed
            all_passed = all(quality_checks.values())
            
            # Log the results
            print("=== DATA QUALITY CHECK RESULTS ===")
            for check, result in quality_checks.items():
                status = "PASSED" if result else "FAILED"
                print(f"{check}: {status}")
            
            if not all_passed:
                print("WARNING: Some data quality checks failed!")
            else:
                print("SUCCESS: All data quality checks passed!")
            
            # Push results to XCom
            ti.xcom_push(key='quality_check_results', value=quality_checks)
            
            return all_passed
        except Exception as e:
            print(f"Error in data quality check: {e}")
            raise
    
    # Task to check data quality
    check_quality = PythonOperator(
        task_id='check_quality',
        python_callable=check_data_quality,
        provide_context=True,
    )
    
    # Define a task to log the pipeline completion
    pipeline_completed = BashOperator(
        task_id='pipeline_completed',
        bash_command='echo "ETL pipeline completed successfully at $(date)"',
    )
    
    # Define the task dependencies
    create_directories >> download_csv >> validate_csv >> transform_csv >> analyze_data_task >> check_quality >> pipeline_completed