from airflow import DAG
from operators.api2db import Api2DbOperator
from operators.transform import TransformOperator
from airflow.operators.python import PythonOperator
# from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
# from custom.docker_xcom_operator import DockerXComOperator
# from airflow.asl_utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from datetime import datetime
# import os
# from docker.types import Mount
# from ClinicalTrialETL.etl.extract import extract_redcap_data
from ClinicalTrialETL.redcap_api import api
# from ClinicalTrialETL.etl import transform, load, utils


with DAG('2019_0361_etl_dag', schedule_interval=None, start_date=datetime(2021, 8, 1), catchup=False) as dag:
    CONN_ID = 'schrage_lab_db'

    with TaskGroup(group_id='extract') as extract_tg:
        extract_prescreening_survey = Api2DbOperator(
            task_id='extract-prescreening-survey',
            conn_id=CONN_ID,
            table='irb_2019_0361_prescreening_survey_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.insulin_resistance_in_adolescents_survey.name],
                'events': [
                    api.Events.prescreening_arm_1.name
                ]
            }
        )

        # this extracts data from select screening forms that were originally completed in-person but were later
        # replaced by online survey
        extract_yogtt004_demographics = Api2DbOperator(
            task_id='extract-yogtt004_demographics',
            conn_id=CONN_ID,
            table='irb_2019_0361_yogtt004_demographics_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.yogtt004_demographics.name,
                ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        extract_yogtt005_medical_history = Api2DbOperator(
            task_id='extract-yogtt005-medical-history',
            conn_id=CONN_ID,
            table='irb_2019_0361_yogtt005_medical_history_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.yogtt005_medical_history.name,
                ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        extract_yogtt006_mri_safety_questionnaire = Api2DbOperator(
            task_id='extract-yogtt006-mri-safety-questionnaire',
            conn_id=CONN_ID,
            table='irb_2019_0361_yogtt006_mri_safety_questionnaire_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.yogtt006_mri_safety_questionnaire.name,
                ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        extract_yogtt007_3_day_physical_activity_recall = Api2DbOperator(
            task_id='extract-yogtt007-3-day-physical-activity-recall',
            conn_id=CONN_ID,
            table='irb_2019_0361_yogtt007_3_day_physical_activity_recall_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.yogtt007_3_day_physical_activity_recall.name,
                ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        extract_yogtt008_tanner_questionnaire = Api2DbOperator(
            task_id='extract-yogtt008_tanner_questionnaire',
            conn_id=CONN_ID,
            table='irb_2019_0361_yogtt008_tanner_questionnaire_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.yogtt008_tanner_questionnaire.name,
                ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        # this extracts all data from the online screening process that changed 2020
        extract_demographics_survey = Api2DbOperator(
            task_id='extract-demographics-survey',
            conn_id=CONN_ID,
            table='irb_2019_0361_demographics_survey_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.demographics_survey.name
                  ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        extract_medical_history_survey = Api2DbOperator(
            task_id='extract-medical-history-survey',
            conn_id=CONN_ID,
            table='irb_2019_0361_medical_history_survey_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.medical_history_survey.name
                ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        extract_mri_survey = Api2DbOperator(
            task_id='extract-mri-survey',
            conn_id=CONN_ID,
            table='irb_2019_0361_mri_survey_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.mri_survey.name,
                ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        extract_par_survey = Api2DbOperator(
            task_id='extract-par-survey',
            conn_id=CONN_ID,
            table='irb_2019_0361_par_survey_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.par_survey.name
                ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        extract_tanner_survey = Api2DbOperator(
            task_id='extract-tanner-survey',
            conn_id=CONN_ID,
            table='irb_2019_0361_tanner_survey_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.tanner_survey.name
                ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        # this extracts the forms/data related to conducting the in-person screening
        extract_screening_data = Api2DbOperator(
            task_id='extract-screening-data',
            conn_id=CONN_ID,
            table='irb_2019_0361_screening_data_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.yogtt002_screening_visit_checklist.name,
                    api.Forms.yogtt009_screening_visit_data_collection_form.name,
                    api.Forms.yogtt010_eligibility_criteria_form.name
                ],
                'events': [
                    api.Events.screening_arm_1.name,
                    api.Events.rescreening_arm_1.name
                ]
            }
        )

        extract_cognitive_data = Api2DbOperator(
            task_id='extract-cognitive-data',
            conn_id=CONN_ID,
            table='irb_2019_0361_cognitive_data_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.yogtt011_cognitive_study_visit_protocol_checklist.name,
                    api.Forms.cognitive_scores.name
                ],
                'events': [
                    api.Events.cognitive_testing_arm_1.name,
                    api.Events.recognitive_testin_arm_1.name
                ]
            }
        )

        extract_mri_structural_data = Api2DbOperator(
            task_id='extract-mri-structural-data',
            conn_id=CONN_ID,
            table='irb_2019_0361_mri_structural_data_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.yogtt012_mri_structural_visit_checklist.name
                ],
                'events': [
                    api.Events.mri_structural_vis_arm_1.name,
                    api.Events.remri_structural_v_arm_1.name
                ]
            }
        )

        extract_ogtt_data = Api2DbOperator(
            task_id='extract-ogtt-data',
            conn_id=CONN_ID,
            table='irb_2019_0361_ogtt_data_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.yogtt013_ogtt_mri_study_visit_protocol_checklist.name,
                    api.Forms.insulin_data.name
                ],
                'events': [
                    api.Events.ogttmri_visit_arm_1.name,
                    api.Events.reogttmri_visit_arm_1.name
                ]
            }
        )

        extract_dexa_data = Api2DbOperator(
            task_id='extract-dexa-data',
            conn_id=CONN_ID,
            table='irb_2019_0361_dexa_data_STG',
            python_callable=api.export_records,
            op_kwargs={
                'fields': ['record_id'],
                'forms': [
                    api.Forms.dexa_data.name
                ],
                'events': [
                    api.Events.dexa_data_arm_1.name,
                    api.Events.redexa_data_arm_1.name
                ]
            }
        )

        # todo: extract compliance data

    with TaskGroup(group_id='transform') as transform_tg:
        # take the raw data from staging table(s), clean it, and stage it for db loading

        transform_subjects = TransformOperator(
            conn_id=CONN_ID,
            table='irb_2019_0361_prescreening_survey_STG',
            sql=f"""SELECT ("""
        )

    #     obj = transform.CleanDemographicsData()
    #     clean_demographics_data = PythonOperator(
    #         task_id='clean-demographics-data',
    #         python_callable=obj,
    #         op_args=[
    #             "{{ ti.xcom_pull(task_ids='extract.extract-redcap-demographics-data') }}",
    #             f'demographics_{TIMESTAMP}.{FILE_EXT}',
    #             "{{ ti.xcom_pull(task_ids='initialization.set-proc-staging-location') }}"
    #         ],
    #     )
    #
    #     [set_proc_staging_location, extract_redcap_demographics_data] >> clean_demographics_data
    #
    #     # this task aggregates all the subjects, visits, and associated visit dates into one output
    #     clean_visits_data = DummyOperator(
    #         task_id='clean-visits-data'
    #     )
    #     set_proc_staging_location >> clean_visits_data
    #
    #     # todo: build and call cleaning method/class that will then parse the file for loading
    #     clean_screening_data = DummyOperator(
    #         task_id='clean-screening-data'
    #     )
    #
    #     [set_proc_staging_location, extract_redcap_screening_data] >> clean_screening_data
    #
    #     clean_vo2_data = DummyOperator(
    #         task_id='clean-vo2-data'
    #     )
    #
    #     [set_proc_staging_location, extract_redcap_vo2_data] >> clean_vo2_data
    #
    #     clean_dexa_data = DummyOperator(
    #         task_id='clean-dexa-data'
    #     )
    #
    #     [set_proc_staging_location, extract_redcap_dexa_data] >> clean_dexa_data
    #
    #     clean_ogtt_bloods = DummyOperator(
    #         task_id='clean-ogtt-bloods'
    #     )
    #
    #     [set_proc_staging_location, extract_redcap_ogtt_data] >> clean_ogtt_bloods
    #
    #     clean_ogtt_vitals = DummyOperator(
    #         task_id='clean-ogtt-vitals'
    #     )
    #
    #     [set_proc_staging_location, extract_redcap_ogtt_data] >> clean_ogtt_vitals
    #
    # with TaskGroup(group_id='load') as load_tg:
    #     load_demographics_data = PythonOperator(
    #         task_id='load-demographics-data',
    #         python_callable=load.load_subjects,
    #         op_kwargs={
    #             'conn_id': "schrage_lab_db",
    #             'csv_path': "{{ ti.xcom_pull(task_ids='transform.clean-demographics-data') }}"
    #         }
    #     )
    #
    #     clean_demographics_data >> load_demographics_data
    #
    #     load_visits_data = DummyOperator(
    #         task_id='load-visits-data'
    #     )
    #
    #     [load_demographics_data, clean_visits_data] >> load_visits_data
    #
    #     load_cbc_data = DummyOperator(
    #         task_id='load-cbc-data'
    #     )
    #
    #     [load_visits_data, clean_screening_data] >> load_cbc_data
    #
    #     load_dexa_data = DummyOperator(
    #         task_id='load-dexa-data'
    #     )
    #
    #     [load_visits_data, clean_dexa_data] >> load_dexa_data
    #
    #     load_vo2_data = DummyOperator(
    #         task_id='load-vo2-data'
    #     )
    #
    #     [load_visits_data, clean_vo2_data] >> load_vo2_data
    #
    #     load_ogtt_bloods_data = DummyOperator(
    #         task_id='load-ogtt-bloods-data'
    #     )
    #
    #     [load_visits_data, clean_ogtt_bloods] >> load_ogtt_bloods_data
    #
    #     load_screening_bloods_data = DummyOperator(
    #         task_id='load-screening-bloods-data'
    #     )
    #
    #     [load_visits_data, clean_screening_data] >> load_screening_bloods_data
    #
    #     load_screening_vitals_data = DummyOperator(
    #         task_id='load-screening-vitals-data'
    #     )
    #
    #     [load_visits_data, clean_screening_data] >> load_screening_vitals_data
    #
    #     # todo: make this table in the db
    #     load_ogtt_vitals = DummyOperator(
    #         task_id='load-ogtt-vitals'
    #     )
    #
    #     [load_visits_data, clean_ogtt_vitals] >> load_ogtt_vitals
    #
    #     # load_to_redcap_0361 = DummyOperator(
    #     #     task_id='load-to-redcap-0361'
    #     # )
    #     #
    #     # transform_tg >> load_to_redcap_0361
    #
    # with TaskGroup(group_id='cleanup') as cleanup_tg:
    #     remove_old_files = DummyOperator(
    #         task_id='remove-old-files'
    #     )
    #     [load_demographics_data, load_visits_data, load_dexa_data, load_vo2_data, load_ogtt_bloods_data,
    #      load_screening_bloods_data, load_screening_vitals_data, load_ogtt_vitals] >> remove_old_files
    #
    # # with TaskGroup(group_id='cleanup') as cleanup_tg:
    # #     remove_old_raw_files = PythonOperator(
    # #         task_id='remove-old-raw-files',
    # #         python_callable=transform.remove_old_files,
    # #         op_kwargs={'key': 'raw_staging_location',
    # #                    'task_ids': ['extract.extract-redcap-data-full',
    # #                                 'extract.extract-form-event-mapping',
    # #                                 'extract.extract-field-names',
    # #                                 'extract.extract-redcap-metadata']}
    # #     )
    # #
    # #     load_tg >> remove_old_raw_files
    # #
    # #     remove_old_files = PythonOperator(
    # #         task_id='remove-old-proc-files',
    # #         python_callable=transform.remove_old_files,
    # #         op_kwargs={'key': 'proc_staging_location',
    # #                    'task_ids': ['transform.set-proc-staging-location']}
    # #     )
    # #
    # #     load_tg >> remove_old_files
