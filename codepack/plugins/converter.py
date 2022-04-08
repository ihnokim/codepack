lines = ['from datetime import datetime',
         'from airflow import DAG',
         'from airflow.operators.python_operator import PythonOperator', '']

for c in codepack.codes.values():
    lines += ['', c.source]

for c in codepack.codes.values():
    lines.append("%s_operator = PythonOperator(task_id='%s', python_callable=%s, dag=dag)" % (c.id, c.id, c.function.__name__))

with open('dag_test.dag', 'w') as f:
    f.writelines([line + '\n' for line in lines])
