from airflow.sdk import task, dag
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag (

    dag_id = "cron_dag",
    start_date = datetime(year=2026, month=8, day=1, tz ="UTC"),
    #schedule = "@daily",
    schedule = CronTriggerTimetable("0 16 * * MON-FRI", timezone="UTC"),
    is_paused_upon_creation = False,
    catchup = True
)

def cron_dag():

    @task.python
    def extract_data(**kwargs):
        print("Extracting Data ......")
        ti = kwargs['ti']
        extract_data_dict = {"api_extracted_data": [1,2,3],
                             "db_extracted_data": [4,5,6],
                             "s3_extracted_data": [7,8,9],
                             "weekend_flag": True}
        ti.xcom_push(key ="extracted_data" , value = extract_data_dict)

    @task.python
    def transform_task_api(**kwargs):
        print("Transforming API data............")
        ti = kwargs['ti']
        api_extracted_data = ti.xcom_pull(key ="extracted_data", task_ids = 'extract_data')["api_extracted_data"]
        api_transformed_data = [x*2 for x in api_extracted_data]
        ti.xcom_push(key ="transformed_data" , value = api_transformed_data)

    @task.python
    def transform_task_db(**kwargs):
        print("Transforming DB data............")
        ti = kwargs['ti']
        db_extracted_data = ti.xcom_pull(key ="extracted_data", task_ids = 'extract_data')["db_extracted_data"]
        db_transformed_data = [x*3 for x in db_extracted_data]
        ti.xcom_push(key ="transformed_data" , value = db_transformed_data)

    @task.python
    def transform_task_s3(**kwargs):
        print("Transforming S3 data............")
        ti = kwargs['ti']
        s3_extracted_data = ti.xcom_pull(key ="extracted_data" , task_ids ='extract_data')["s3_extracted_data"]
        s3_transformed_data = [x*4 for x in s3_extracted_data]
        ti.xcom_push(key ="transformed_data" , value = s3_transformed_data)

    @task.branch
    def decider_task(**kwargs):
        ti = kwargs['ti']
        weekend_flag = ti.xcom_pull(key ="extracted_data", task_ids ="extract_data")["weekend_flag"]
        if weekend_flag == True:
            return 'final_data_task'
        else:
            return 'final_print_task'
        

    @task.python
    def final_data_task(**kwargs):
        print("Loading data to destination............")  
        ti = kwargs['ti']
        final_data = ti.xcom_pull(key ="transformed_data", task_ids = ["transform_task_api", "transform_task_db", "transform_task_s3"])
        final_data_flat = [item for sub_list in final_data for item in sub_list]
        ti.xcom_push(key ="final_value" , value = final_data_flat)
                                  
    @task.bash
    def final_print_task(**kwargs):
        ti = kwargs['ti']
        final_data = ti.xcom_pull(key ="transformed_data", task_ids = ["transform_task_api", "transform_task_db", "transform_task_s3"])
        final_data_flat = [item for sub_list in final_data for item in sub_list]
        return f"echo 'Loaded data: {final_data_flat}'"
          

    first = extract_data()
    second = transform_task_api()
    third = transform_task_db()
    fourth = transform_task_s3()
    fifth = final_data_task()
    sixth = final_print_task()
    decider = decider_task()

    first >> [second , third , fourth]
    [second , third , fourth] >> decider
    decider >> [fifth, sixth]
    


cron_dag()