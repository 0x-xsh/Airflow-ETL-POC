from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'me',
    'start_date': datetime.today(),
    'email': 'myemail@maill.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='ETL_toll_data',
    schedule_interval='@daily',
    default_args=default_args,
    description='Apache Airflow Final Assignment'

) as dag:
    
    dest_dir = '/home/airflow/dags/poc'
    
    unzip_data = BashOperator(
        task_id='unzip_data',
        bash_command=f'tar -zxvf {dest_dir}/tolldata.tgz -C {dest_dir}'
    )

    extract_data_from_csv = BashOperator(
        task_id='extract_data_from_csv',
        bash_command= f"cut -d',' -f1,2,3,4 {dest_dir}/vehicle-data.csv > {dest_dir}/csv_data.csv"
    )

    extract_data_from_tsv = BashOperator(
        task_id='extract_data_from_tsv',
        bash_command= f"cut -d$'\t' -f5,6,7 {dest_dir}/tollplaza-data.tsv | tr '\t' ',' > {dest_dir}/tsv_data.csv"
    )
    
    extract_data_from_fixed_width = BashOperator(
        task_id='extract_data_from_fixed_width',
        bash_command=f"tr -s ' ' < {dest_dir}/payment-data.txt | cut -d' ' -f11,12 | tr ' ' ',' > {dest_dir}/fixed_width_data.csv"
    )

    consolidate_data = BashOperator(
        task_id = 'consolidate_data',
        bash_command = f'paste {dest_dir}/csv_data.csv {dest_dir}/tsv_data.csv {dest_dir}/fixed_width_data.csv > {dest_dir}/extracted_data.csv'
    )

    transform_data = BashOperator(
        task_id = 'transform_data',
        
        bash_command = f"awk '$5 = toupper($5)' {dest_dir}/extracted_data.csv > {dest_dir}/staging/transformed_data.csv"
                    


    )



    unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data
