# IRB 2019-0361 DAGs

## Pipelines

## Graphs

## Notes
Only Airflow DAG code lives in `~/airflow/dags/` to avoid negative impacts on the scheduler. To make the ETL modules (i.e.
the workhorse code) available to the DAGs, a `.pth` file was created in the `site-packages` directory. References: 
https://stackoverflow.com/questions/17236675/how-to-make-my-python-module-available-system-wide-on-linux
https://docs.python.org/2/library/site.html#module-site
