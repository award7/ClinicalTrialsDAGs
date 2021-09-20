from airflow import DAG
# from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
# # from airflow.providers.mongo.hooks.mongo import MongoHook
# from custom.mongodb_operator import S3ToMongoOperator
# from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
# # from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# from custom.docker_xcom_operator import DockerXComOperator
# from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from datetime import datetime
# import os
# from docker.types import Mount


with DAG('2019_0361_etl_dag', schedule_interval=None, start_date=datetime(2021, 8, 1), catchup=False) as dag:
    with TaskGroup(group_id='extract') as extract_tg:
        extract_prescreening_data = DummyOperator(
            task_id='extract-prescreening-data'
        )

        extract_screening_data = DummyOperator(
            task_id='extract-screening-data'
        )

        extract_cognitive_data = DummyOperator(
            task_id='extract-cognitive-data'
        )

        extract_structural_mri_visit_data = DummyOperator(
            task_id='extract-structural-mri-visit-data'
        )

        extract_ogtt_visit_data = DummyOperator(
            task_id='extract-ogtt-visit-data'
        )

        extract_medication_data = DummyOperator(
            task_id='extract-medication-data'
        )

    with TaskGroup(group_id='transform') as transform_tg:
        merge_data = DummyOperator(
            task_id='merge-data'
        )
        extract_tg >> merge_data

        calculate_mets = DummyOperator(
            task_id='calculate_mets'
        )
        merge_data >> calculate_mets

        calculate_homa_ir = DummyOperator(
            task_id='calculate-homa-ir'
        )
        merge_data >> calculate_homa_ir

        aggregate_rx = DummyOperator(
            task_id='aggregate-rx'
        )
        merge_data >> aggregate_rx

        aggregate_ae = DummyOperator(
            task_id='aggregate-ae'
        )

        merge_data >> aggregate_ae

        # calculate_ogtt_glucose_auc = DummyOperator(
        #     task_id='calculate-ogtt-glucose-auc'
        # )
        #
        # calculate_ogtt_insulin_auc = DummyOperator(
        #     task_id='calculate-ogtt-insulin-auc'
        # )

    with TaskGroup(group_id='load') as load_tg:
        load_to_redcap_0361 = DummyOperator(
            task_id='load-to-redcap-0361'
        )
        transform_tg >> load_to_redcap_0361

        # 0838
        # load to msssql

