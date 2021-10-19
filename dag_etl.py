from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
# # from airflow.providers.mongo.hooks.mongo import MongoHook
# from custom.mongodb_operator import S3ToMongoOperator
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator
# # from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
# from custom.docker_xcom_operator import DockerXComOperator
# from airflow.asl_utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from datetime import datetime
# import os
# from docker.types import Mount
from ClinicalTrialETL.etl.extract import extract_redcap_data
from ClinicalTrialETL.redcap_api.api import Events, Forms
from ClinicalTrialETL.etl import transform, load, utils


with DAG('2019_0361_etl_dag', schedule_interval=None, start_date=datetime(2021, 8, 1), catchup=False) as dag:
    with TaskGroup(group_id='initialization') as init_tg:
        # set the proc staging location in an xcom
        set_proc_staging_location = PythonOperator(
            task_id='set-proc-staging-location',
            python_callable=utils.get_default_staging_location,
            op_kwargs={
                'bucket': 'proc'
            }
        )

    with TaskGroup(group_id='extract') as extract_tg:

        TIMESTAMP = datetime.now().strftime('%Y%m%d_%I%M%S')
        FILE_EXT = 'csv'

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
                'fields': ['record_id', 'age_scr_data'],
                'forms': [Forms.demographics_survey.name, Forms.yogtt004_demographics.name],
                'events': [Events.screening_arm_1.name, Events.rescreening_arm_1.name],
                'filter_logic': '[yn_eligible]=1'
                       }
        )

        # extract screening data
        extract_redcap_screening_data = PythonOperator(
            task_id='extract-redcap-screening-data',
            python_callable=extract_redcap_data,
            op_kwargs={
                'file_name': f'screening_data_raw_{TIMESTAMP}.{FILE_EXT}',
                'fields': ['record_id'],
                'forms': [Forms.yogtt009_screening_visit_data_collection_form.name],
                'events': [Events.screening_arm_1.name, Events.rescreening_arm_1.name],
                'filter_logic': '[yn_eligible]=1'
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
                'filter_logic': '[yogtt012_mri_structural_visit_checklist_complete]=2'
            }
        )

        # extract ogtt data
        extract_redcap_ogtt_data = PythonOperator(
            task_id='extract-redcap-ogtt-data',
            python_callable=extract_redcap_data,
            op_kwargs={
                'file_name': f'ogtt_data_raw_{TIMESTAMP}.{FILE_EXT}',
                'fields': ['record_id'],
                'forms': [Forms.yogtt013_ogtt_mri_study_visit_protocol_checklist.name, Forms.insulin_data.name],
                'events': [Events.ogttmri_visit_arm_1.name, Events.reogttmri_visit_arm_1.name],
                'filter_logic': '[completed_ogtt]=2'
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
                'filter_logic': '[yogtt011_cognitive_study_visit_protocol_checklist_complete]=2'
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
                'filter_logic': '[dexa_data_complete]=2'
            }
        )

        # extract vo2 data
        # todo: build form in redcap for vo2 data
        extract_redcap_vo2_data = DummyOperator(
            task_id='extract-redcap-vo2-data',
            # python_callable=extract_redcap_data,
            # op_kwargs={
            #     'file_name': f'vo2_data_raw_{TIMESTAMP}.{FILE_EXT}',
            #     'fields': ['record_id'],
            #     'forms': [Forms.name],
            #     'events': [Events.dexa_data_arm_1.name, Events.redexa_data_arm_1.name],
            #     'filter_logic': EXTRACT_FILTER_LOGIC
            # }
        )

    with TaskGroup(group_id='transform') as transform_tg:
        # take the raw api file and parse it into the necessary components for db loading

        obj = transform.CleanDemographicsData()
        clean_demographics_data = PythonOperator(
            task_id='clean-demographics-data',
            python_callable=obj,
            op_args=[
                "{{ ti.xcom_pull(task_ids='extract.extract-redcap-demographics-data') }}",
                f'demographics_{TIMESTAMP}.{FILE_EXT}',
                "{{ ti.xcom_pull(task_ids='initialization.set-proc-staging-location') }}"
            ],
        )

        [set_proc_staging_location, extract_redcap_demographics_data] >> clean_demographics_data

        # this task aggregates all the subjects, visits, and associated visit dates into one output
        clean_visits_data = DummyOperator(
            task_id='clean-visits-data'
        )
        set_proc_staging_location >> clean_visits_data

        # todo: build and call cleaning method/class that will then parse the file for loading
        clean_screening_data = DummyOperator(
            task_id='clean-screening-data'
        )

        [set_proc_staging_location, extract_redcap_screening_data] >> clean_screening_data

        clean_vo2_data = DummyOperator(
            task_id='clean-vo2-data'
        )

        [set_proc_staging_location, extract_redcap_vo2_data] >> clean_vo2_data

        clean_dexa_data = DummyOperator(
            task_id='clean-dexa-data'
        )

        [set_proc_staging_location, extract_redcap_dexa_data] >> clean_dexa_data

        clean_ogtt_bloods = DummyOperator(
            task_id='clean-ogtt-bloods'
        )

        [set_proc_staging_location, extract_redcap_ogtt_data] >> clean_ogtt_bloods

        clean_ogtt_vitals = DummyOperator(
            task_id='clean-ogtt-vitals'
        )

        [set_proc_staging_location, extract_redcap_ogtt_data] >> clean_ogtt_vitals

    with TaskGroup(group_id='load') as load_tg:
        load_demographics_data = PythonOperator(
            task_id='load-demographics-data',
            python_callable=load.load_subjects,
            op_kwargs={
                'conn_id': "schrage_lab_db",
                'csv_path': "{{ ti.xcom_pull(task_ids='transform.clean-demographics-data') }}"
            }
        )

        clean_demographics_data >> load_demographics_data

        load_visits_data = DummyOperator(
            task_id='load-visits-data'
        )

        [load_demographics_data, clean_visits_data] >> load_visits_data

        load_cbc_data = DummyOperator(
            task_id='load-cbc-data'
        )

        [load_visits_data, clean_screening_data] >> load_cbc_data

        load_dexa_data = DummyOperator(
            task_id='load-dexa-data'
        )

        [load_visits_data, clean_dexa_data] >> load_dexa_data

        load_vo2_data = DummyOperator(
            task_id='load-vo2-data'
        )

        [load_visits_data, clean_vo2_data] >> load_vo2_data

        load_ogtt_bloods_data = DummyOperator(
            task_id='load-ogtt-bloods-data'
        )

        [load_visits_data, clean_ogtt_bloods] >> load_ogtt_bloods_data

        load_screening_bloods_data = DummyOperator(
            task_id='load-screening-bloods-data'
        )

        [load_visits_data, clean_screening_data] >> load_screening_bloods_data

        load_screening_vitals_data = DummyOperator(
            task_id='load-screening-vitals-data'
        )

        [load_visits_data, clean_screening_data] >> load_screening_vitals_data

        # todo: make this table in the db
        load_ogtt_vitals = DummyOperator(
            task_id='load-ogtt-vitals'
        )

        [load_visits_data, clean_ogtt_vitals] >> load_ogtt_vitals

        # load_to_redcap_0361 = DummyOperator(
        #     task_id='load-to-redcap-0361'
        # )
        #
        # transform_tg >> load_to_redcap_0361

    with TaskGroup(group_id='cleanup') as cleanup_tg:
        remove_old_files = DummyOperator(
            task_id='remove-old-files'
        )
        [load_demographics_data, load_visits_data, load_dexa_data, load_vo2_data, load_ogtt_bloods_data,
         load_screening_bloods_data, load_screening_vitals_data, load_ogtt_vitals] >> remove_old_files

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
