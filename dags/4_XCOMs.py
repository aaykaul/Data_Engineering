from airflow.sdk import dag, task

@dag(
        dag_id ="xcom_dag_auto"
)
def xcom_dag_auto():
    @task.python
    def first_task():
        print("Extracting data....This is the first task")
        fetched_data= {"nums": [1,2,3,4,5]}
        return fetched_data

    @task.python
    def second_task(data:dict):
        print("Transform data....This is the second task")
        fetched_data = data['nums']
        transformed_data = fetched_data * 2
        transformed_data_dict = {"transformed_nums": transformed_data}
        return transformed_data_dict

    @task.python
    def third_task(data:dict):
        loaded_data = data['transformed_nums']
        return loaded_data

    first = first_task()
    second = second_task(first)
    third = third_task(second)

    # Airflow can identify it automatically because of how we pass parameters here
    #first >> second >> third

xcom_dag_auto()