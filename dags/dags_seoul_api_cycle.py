from operators.seoul_api_to_csv_operator import SeoulApiToCsvOperator
from airflow import DAG
import pendulum

with DAG(
    dag_id='dags_seoul_api_cycle',
    schedule='0 7 * * *',
    start_date=pendulum.datetime(2024,6,1, tz='Asia/Seoul'),
    catchup=False
) as dag:
    ''' 서울시 공공자전거 이용현황 '''
    tb_cycle_using_status = SeoulApiToCsvOperator(
        task_id = 'tb_cycle_using_status',
        dataset_nm='tbCycleUseStatus',
        path='/opt/airflow/files/tbCycleUseStatus/{{data_interval_end.in_timezone("Asia/Seoul") | ds_nodash}}',
        file_name='tbCycleUseStatus.csv',
        base_dt='2024-06-01'
    )

    tb_cycle_using_status