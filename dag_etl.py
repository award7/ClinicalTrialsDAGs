from airflow import DAG
from airflow.operators.python import PythonOperator
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
from ClinicalTrialETL.etl.extract import extract_redcap_data
# from ClinicalTrialETL.redcap_api.api import Events, Forms
from ClinicalTrialETL.etl import transform


with DAG('2019_0361_etl_dag', schedule_interval=None, start_date=datetime(2021, 8, 1), catchup=False) as dag:
    with TaskGroup(group_id='extract') as extract_tg:
        TIMESTAMP = datetime.now().strftime('%Y%m%d_%I%M%S')
        FILE_EXT = 'csv'

        # extract all data to store as a backup
        extract_redcap_data_full = PythonOperator(
            task_id='extract-redcap-data-full',
            python_callable=extract_redcap_data
        )

        extract_form_event_mapping = PythonOperator(
            task_id='extract-form-event-mapping',
            python_callable=extract_redcap_data,
            op_kwargs={'export_content': 'form_event_mapping',
                       'file_name': f'irb_2019_0361_form_event_mapping_{TIMESTAMP}.{FILE_EXT}'}
        )

        extract_field_names = PythonOperator(
            task_id='extract-field-names',
            python_callable=extract_redcap_data,
            op_kwargs={'export_content': 'field_names',
                       'file_name': f'irb_2019_0361_field_names_{TIMESTAMP}.{FILE_EXT}'}
        )

        extract_redcap_metadata = PythonOperator(
            task_id='extract-redcap_metadata',
            python_callable=extract_redcap_data,
            op_kwargs={'export_content': 'metadata',
                       'file_name': 'irb_2019_0361_metadata_{TIMESTAMP}.{FILE_EXT}'}
        )

    with TaskGroup(group_id='transform') as transform_tg:
        # take the raw api file and parse it into the necessary components for db loading

        # set the proc staging location in an xcom
        set_proc_staging_location = PythonOperator(
            task_id='set-proc-staging-location',
            python_callable=transform.set_proc_staging_location
        )

        extract_tg >> set_proc_staging_location

        event_form_field_mapping = DummyOperator(
            task_id='event-form-field-mapping'
        )

        extract_tg >> event_form_field_mapping

        parse_prescreening_data = PythonOperator(
            task_id='parse-prescreening-data',
            python_callable=transform.parse_prescreening_data,
            op_kwargs={'file_name': f'prescreening_data_{TIMESTAMP}.{FILE_EXT}'}
        )

        [set_proc_staging_location, event_form_field_mapping] >> parse_prescreening_data

        get_prescreening_survey_counts = PythonOperator(
            task_id='get-prescreening-survey-counts',
            python_callable=transform.get_opened_survey_count
        )

        parse_prescreening_data >> get_prescreening_survey_counts



        # calculate_mets = DummyOperator(
        #     task_id='calculate_mets'
        # )
        # parse_data >> calculate_mets
        #
        # calculate_homa_ir = DummyOperator(
        #     task_id='calculate-homa-ir'
        # )
        # parse_data >> calculate_homa_ir
        #
        # aggregate_rx = DummyOperator(
        #     task_id='aggregate-rx'
        # )
        # parse_data >> aggregate_rx
        #
        # aggregate_ae = DummyOperator(
        #     task_id='aggregate-ae'
        # )
        #
        # parse_data >> aggregate_ae

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

