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
from ClinicalTrialETL.redcap_api.api import Events, Forms
from ClinicalTrialETL.etl import transform


with DAG('2019_0361_etl_dag', schedule_interval=None, start_date=datetime(2021, 8, 1), catchup=False) as dag:
    with TaskGroup(group_id='initialization') as init_tg:
        # set the proc staging location in an xcom
        set_proc_staging_location = PythonOperator(
            task_id='set-proc-staging-location',
            python_callable=transform.set_proc_staging_location
        )

    with TaskGroup(group_id='extract') as extract_tg:

        TIMESTAMP = datetime.now().strftime('%Y%m%d_%I%M%S')
        FILE_EXT = 'csv'

        # only retrieve those signed off as eligible
        EXTRACT_FILTER_LOGIC = '[yn_eligible]=1'

        # extract all data to store as a backup
        extract_redcap_data_full = PythonOperator(
            task_id='extract-redcap-data-full',
            python_callable=extract_redcap_data
        )

        # extract demographics
        extract_redcap_demographics_data = PythonOperator(
            task_id='extract-redcap-demographics-data',
            python_callable=extract_redcap_data,
            op_kwargs={
                'file_name': f'demographics_raw_{TIMESTAMP}.{FILE_EXT}',
                'fields': ['record_id'],
                'forms': [Forms.demographics_survey.name, Forms.yogtt004_demographics.name],
                'events': [Events.screening_arm_1.name, Events.rescreening_arm_1.name],
                'filter_logic': EXTRACT_FILTER_LOGIC
                       }
        )

        # extract screening data
        extract_redcap_screening_data = PythonOperator(
            task_id='extract-redcap-screening-data',
            python_callable=extract_redcap_data,
            op_kwargs={
                'file_name': f'screening_data_raw_{TIMESTAMP}.{FILE_EXT}',
                'fields': ['record_id'],
                'forms': [Forms.yogtt002_screening_visit_checklist.name, Forms.yogtt009_screening_visit_data_collection_form.name],
                'events': [Events.screening_arm_1.name, Events.rescreening_arm_1.name],
                'filter_logic': EXTRACT_FILTER_LOGIC
            }
        )

        # extract mri structural data
        extract_redcap_mri_structural_data = PythonOperator(
            task_id='extract-redcap-mri-structural-data',
            python_callable=extract_redcap_data,
            op_kwargs={
                'file_name': f'mri_structural_data_raw_{TIMESTAMP}.{FILE_EXT}',
                'fields': ['record_id'],
                'forms': [Forms.yogtt012_mri_structural_visit_checklist.name],
                'events': [Events.mri_structural_vis_arm_1.name, Events.remri_structural_v_arm_1.name],
                'filter_logic': EXTRACT_FILTER_LOGIC
            }
        )

        # extract ogtt data
        extract_redcap_ogtt_data = PythonOperator(
            task_id='extract-redcap-ogtt-data',
            python_callable=extract_redcap_data,
            op_kwargs={
                'file_name': f'ogtt_data_raw_{TIMESTAMP}.{FILE_EXT}',
                'fields': ['record_id'],
                'forms': [Forms.yogtt013_ogtt_mri_study_visit_protocol_checklist.name],
                'events': [Events.ogttmri_visit_arm_1.name, Events.reogttmri_visit_arm_1.name],
                'filter_logic': EXTRACT_FILTER_LOGIC
            }
        )

        # extract cognitive data
        extract_redcap_cognitive_data = PythonOperator(
            task_id='extract-redcap-cognitive-data',
            python_callable=extract_redcap_data,
            op_kwargs={
                'file_name': f'cognitive_data_raw_{TIMESTAMP}.{FILE_EXT}',
                'fields': ['record_id'],
                'forms': [Forms.yogtt011_cognitive_study_visit_protocol_checklist.name,
                          Forms.cognitive_scores.name],
                'events': [Events.cognitive_testing_arm_1.name, Events.recognitive_testin_arm_1.name],
                'filter_logic': EXTRACT_FILTER_LOGIC
            }
        )

        # extract dexa data
        extract_redcap_dexa_data = PythonOperator(
            task_id='extract-redcap-dexa-data',
            python_callable=extract_redcap_data,
            op_kwargs={
                'file_name': f'dexa_data_raw_{TIMESTAMP}.{FILE_EXT}',
                'fields': ['record_id'],
                'forms': [Forms.dexa_data.name],
                'events': [Events.dexa_data_arm_1.name, Events.redexa_data_arm_1.name],
                'filter_logic': EXTRACT_FILTER_LOGIC
            }
        )

    with TaskGroup(group_id='transform') as transform_tg:
        # take the raw api file and parse it into the necessary components for db loading

        obj = transform.CleanDemographicsData()
        clean_demographics_data = PythonOperator(
            task_id='clean-demographics-data',
            python_callable=obj,
            op_kwargs={
                'raw_task_id': 'extract.extract-redcap-demographics-data',
                'proc_file_name': f'demographics_{TIMESTAMP}.{FILE_EXT}'
            }
        )

        [set_proc_staging_location, extract_redcap_demographics_data] >> clean_demographics_data

    def test_load():
        import pymssql
        server = ''
        database = ''
        uid = ''
        pwd = ''

        conn = pymssql.connect(server, uid, pwd, database)
        cur = conn.cursor()
        sql = 'SELECT * FROM sys.tables'
        cur.execute(sql)
        for row in cur:
            print(row)

    with TaskGroup(group_id='load') as load_tg:
        load_test = PythonOperator(
            task_id='load-test',
            python_callable=test_load
        )

        clean_demographics_data >> load_test

    # with TaskGroup(group_id='load') as load_tg:
    #     load_to_redcap_0361 = DummyOperator(
    #         task_id='load-to-redcap-0361'
    #     )
    #     transform_tg >> load_to_redcap_0361
    #
    #     # 0838
    #     # load to msssql
    #
    # with TaskGroup(group_id='cleanup') as cleanup_tg:
    #     remove_old_raw_files = PythonOperator(
    #         task_id='remove-old-raw-files',
    #         python_callable=transform.remove_old_files,
    #         op_kwargs={'key': 'raw_staging_location',
    #                    'task_ids': ['extract.extract-redcap-data-full',
    #                                 'extract.extract-form-event-mapping',
    #                                 'extract.extract-field-names',
    #                                 'extract.extract-redcap-metadata']}
    #     )
    #
    #     load_tg >> remove_old_raw_files
    #
    #     remove_old_files = PythonOperator(
    #         task_id='remove-old-proc-files',
    #         python_callable=transform.remove_old_files,
    #         op_kwargs={'key': 'proc_staging_location',
    #                    'task_ids': ['transform.set-proc-staging-location']}
    #     )
    #
    #     load_tg >> remove_old_files
