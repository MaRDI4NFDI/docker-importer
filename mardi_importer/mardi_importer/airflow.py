from mardi_importer.sources import import_source

# Simple sources
cran_task = PythonOperator(
    task_id='import_cran',
    python_callable=import_source,
    op_kwargs={'source_name': 'cran'},
    dag=dag
)

# ZBMath with custom pull/push settings
zbmath_task = PythonOperator(
    task_id='import_zbmath',
    python_callable=import_source,
    op_kwargs={
        'source_name': 'zbmath',
        'pull': False,  # Override default
        'push': True
    },
    dag=dag
)