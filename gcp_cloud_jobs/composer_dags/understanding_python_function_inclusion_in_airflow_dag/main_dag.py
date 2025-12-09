from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator   
from datetime import datetime, timedelta
# -----------------------------
# Python functions - Sec4       
# -----------------------------

default_args =  {
        "owner": "data_engineering_team",
        "depends_on_past": False, 
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,   
        "retry_delay": timedelta(minutes=5)
    }


# function to print adding of two numbers
def add_numbers(a, b):
    print(f"Adding {a} and {b}")
    print(f"The sum is: {a + b}")


def multiply_numbers(x, y):
    print(f"Multiplying {x} and {y}")
    print(f"The product is: {x * y}")

# dag instantiation
with DAG(   
    dag_id="understanding_python_function_inclusion_in_airflow_dag_v2",
    description="DAG to understand how to include python functions in Airflow DAGs",
    start_date=datetime(2025, 12, 5),
    schedule_interval=None,
    default_args=default_args,  
    catchup=False,
    tags=["demo", "python_task"]
):
    # give a task using emty operator
    start = EmptyOperator(task_id="start") 
    # PythonOperator Task to add two numbers
    addition_task = PythonOperator(
        task_id="add_two_numbers_task",
        python_callable=add_numbers,
        op_kwargs={
            "a": 10,
            "b": 20
        }
    )

    multiply_numbers_task = PythonOperator(
        task_id="multiply_two_numbers_task",
        python_callable=multiply_numbers,
        op_kwargs={
            "x": 5,
            "y": 4
        }
    )   
    # give a task using emty operator
    end = EmptyOperator(task_id="end")  
    # TASK DEPENDENCIES
    start >> [addition_task, multiply_numbers_task] >> end