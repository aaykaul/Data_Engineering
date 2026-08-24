from airflow.sdk import dag, task

@dag(
        dag_id ="xcom_dag_manual"
)
def xcom_dag_manual():
    @task.python
    def first_task(**kwargs):
        #extract ti from kwargs to to push XComds manually
        ti = kwargs['ti']

        print("Extracting data....This is the first task")
        fetched_data= {"nums": [1,2,3,4,5]}
        ti.xcom_push(key ='return_result', value = fetched_data)

    @task.python
    def second_task(**kwargs):
        print("Transform data....This is the second task")
        ti = kwargs['ti']

        fetched_data =  ti.xcom_pull(key ='return_result', task_ids = 'first_task')['nums']

        transformed_data = [num * 2 for num in fetched_data]
        transformed_data_dict = {"transformed_nums": transformed_data}
        ti.xcom_push(key ='return_result', value = transformed_data_dict)

    @task.python
    def third_task(**kwargs):
        ti = kwargs['ti']
        loaded_data = ti.xcom_pull(key='return_result', task_ids ='second_task')['transformed_nums']
        ti.xcom_push(key='return_result1', value = loaded_data)
        #return(loaded_data)

    first = first_task()
    second = second_task()
    third = third_task()

    first >> second >> third

xcom_dag_manual()