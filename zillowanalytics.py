from airflow import DAG
from datetime import timedelta, datetime 
import requests
from airflow.operators.python import PythonOperator
import json
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


now = datetime.now()
dt_now_string = now.strftime("%d%m%Y%H%M%S")

#load JSON config file to extract data
with open('/home/ubuntu/airflow/config_api.json','r') as config_file :
 api_host_key = json.load(config_file)

default_args = {
    'owner' : 'airflow',
    'depends_on_past' : False,
    'start_date' : datetime(2023, 8, 1),
    'email' : ['kanchandendge@gmail.com'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'retries' : 2,
    'retry_delay' : timedelta(seconds = 15)
}
#define s3 bucket and file details
s3_bucket = 'cleaned-data-zone'

# define function to extract the zillow data
# kwargs - a dic of keyword arguments that will get unpacked in your function
def extract_zillow_data(**kwargs) : 
    url = kwargs['url']
    headers = kwargs['headers']
    querystring = kwargs['querystring']
    dt_string = kwargs['date_string']
    #return headers
    response = requests.get(url, headers = headers, params = querystring)
    response_data = response.json()

    #Specify the output file path
    output_file_path = f"/home/ubuntu/response_data_{dt_string}.json"
    file_str = f'response_data_{dt_string}.csv'

    #Write the JSON response to a file
    with open(output_file_path, "w") as output_file :
        json.dump(response_data, output_file, indent = 4)
    output_list = [output_file_path, file_str]
    return output_list

with DAG('zillowanalytics_dag',
         default_args = default_args,
         schedule = '@daily',
         catchup = False) as dag:
    
        extract_zillow_data_var = PythonOperator(
            task_id = 'task_extract_zillow_data',
            python_callable = extract_zillow_data,
            op_kwargs = {
                'url' : 'https://zillow56.p.rapidapi.com/search',
                'querystring' : {"location" : "houston, tx"}, 
                'headers' : api_host_key, 
                'date_string' : dt_now_string
                }
            )
  
        load_to_s3 = BashOperator(
            task_id = 'task_load_to_S3',
            #ti.xcom_pull = task instance cross combition
            bash_command = '/home/ubuntu/.local/bin/aws s3 mv {{ ti.xcom_pull("task_extract_zillow_data")[0]}} s3://zillow-analytics-extract/',
            ) 
        
        # is_file_in_s3_available = S3KeySensor(
        #     task_id = 'task_is_in_s3_check',
        #     bucket_key = '{{ti.xcom_pull("task_extract_zillow_data")[1]}}',
        #     bucket_name = s3_bucket,
        #     aws_conn_id = 'aws_s3_conn',
        #     wildcard_match = False,
        #     timeout = 500,
        #     poke_interval = 1,
        # )
        
        extract_zillow_data_var >> load_to_s3  #>> is_file_in_s3_available
