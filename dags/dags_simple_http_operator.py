from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.decorators import task

with DAG(
    dag_id="dags_simple_http_operator",
    schedule=None,
    start_date=pendulum.datetime(2024, 6, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    
    '''서울시 공공자전거 이용현황 '''

    tb_cycle_using_info = SimpleHttpOperator(
        task_id='tb_cycle_using_info',
        http_conn_id='openapi.seoul.go.kr',
        endpoint='{{var.value.apikey_openapi_seoul_go_kr}}/json/tbCycleUseStatus/1/10/2024-06-01',
        method='GET',
        headers={
            'Content-Type':'application/json',
            'charset':'utf-8',
            'Accept':'*/*'
        }
    )

    @task(task_id='python_2')
    def python_2(**kwargs):
        ti = kwargs['ti']
        rslt = ti.xcom_pull(task_ids='tb_cycle_using_info')
        import json
        from pprint import pprint

        pprint(json.loads(rslt))

    tb_cycle_using_info >> python_2()