



import ast
import os
import pandas as pd
import datatest as validate_columns
import time
from datetime import datetime

# timestr = time.strftime("%Y-%m-%d")
timestr = datetime.now().strftime("%Y_%m_%d")


os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'dbt-demos-370908-7309d018d8fd.json'

import subprocess
account = 'gcloud auth activate-service-account dbt-demos-370908@appspot.gserviceaccount.com --key-file=dbt-demos-370908-7309d018d8fd.json --project=dbt-demos-370908'
cmd = f"{account}"

subprocess.run(cmd, shell=True)
def move(source_uri: str,
         destination_uri: str) -> None:
    """
    Move file from source_uri to destination_uri.

    :param source_uri: gs:// - like uri of the source file/directory
    :param destination_uri: gs:// - like uri of the destination file/directory
    :return: None
    """
    cmd = f"gsutil -m mv {source_uri} {destination_uri}"
    subprocess.run(cmd, shell=True)



def copy(source_uri: str,
         destination_uri: str) -> None:
    """
    Move file from source_uri to destination_uri.

    :param source_uri: gs:// - like uri of the source file/directory
    :param destination_uri: gs:// - like uri of the destination file/directory
    :return: None
    """
    cmd = f"gsutil -m cp {source_uri} {destination_uri}"
    subprocess.run(cmd, shell=True)


def delete(source_uri: str) -> None:
    """
    Move file from source_uri to destination_uri.

    :param source_uri: gs:// - like uri of the source file/directory
    :param destination_uri: gs:// - like uri of the destination file/directory
    :return: None
    """
    # gsutil rm gs://bucket/kitten.png
    cmd = f"gsutil rm {source_uri}"
    subprocess.run(cmd, shell=True)


def validate_columns_names(folder_name: str):

    from google.cloud import storage

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'dbt-demos-370908-7309d018d8fd.json'

    client = storage.Client()
    bucket = client.get_bucket('johennes')
    # buckets = client.list_buckets()
    blobs = bucket.list_blobs()
    # buckets = client.list_buckets()
    # # gs://johennes/{timestr}/
    buckets_list_blobs = [item.name for item in blobs]

    list_of_all_texts_files = []
    for item in buckets_list_blobs:
        if item.startswith("Processed_"):
            list_of_all_texts_files.append(item)
        else:
            print('')

    buckets_list_blobs = list_of_all_texts_files



    for a in buckets_list_blobs:
        blob = bucket.blob(f'{a}')

        with blob.open("r") as f:
        # reading the file
            data = f.read()
            data_into_list = data.split("\n")

        passed_csv = []
        failed_csv = []
        for i in data_into_list:

            try:
                data_columns = list(pd.read_csv(f'gs://mphakashj/{i}',nrows=0,storage_options={"token": "dbt-demos-370908-7309d018d8fd.json"} ))
                template_columns = pd.read_csv('gs://mphakashj/df_data.csv',storage_options={"token": "dbt-demos-370908-7309d018d8fd.json"} )
                template_columns = template_columns[template_columns['file_names']==i].reset_index().drop('index', axis = 1)
                x = sorted(ast.literal_eval(template_columns["columns_names"][0]))
                validate_columns.validate(data_columns, x)
                passed_csv.append(i)
            except:
                failed_csv.append(i)

        if len(passed_csv)==len(data_into_list):

            print('data validation passed')
            print ('run pipeline')
            

            x = [f'{a}'.split('/')[1]]
            a_ = [f'{a}'.split('.')[0]]
            folder = [i.split('/')[1] for i in a_][0]

            for y in data_into_list + x:
                move(source_uri = f'gs://mphakashj/{y}', destination_uri = f'gs://johennes/Processed/{folder}/')
                print(33333333333333333333333)
                print(33333333333333333333333)
                print(33333333333333333333333)
                print(x)
                print(a_)
                
            move(source_uri = f'gs://johennes/Processed_/{x[0]}', destination_uri = f'gs://johennes/Processed/{folder}/')
            delete(f'gs://johennes/Processed_/')

            
            
            # delete(source_uri='gs://mphakashj/{a}')

            
            # delete('')
        else:

            x = [f'{a}'.split('/')[1]]
            a_ = [f'{a}'.split('.')[0]]
            folder = [i.split('/')[1] for i in a_][0]

            

            for y in data_into_list + x:
                move(source_uri = f'gs://johennes/Processed_/{y}', destination_uri = f'gs://johennes/failed/{folder}/')
                move(source_uri = f'gs://mphakashj/{y}', destination_uri = f'gs://johennes/failed/{folder}/')

                print(33333333333333333333333)
                print(33333333333333333333333)
                print(33333333333333333333333)
                print(x)
                print(a_)
                # delete(f'gs://johennes/{a_[0]}/{x[0]}')
            delete(f'gs://johennes/Processed_/')

            



            print('Data Vaidation failed')



    return True


def file_name_validation(project: str, bucket: str):

    from google.cloud import storage

    client = storage.Client(project=project)
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = 'dbt-demos-370908-7309d018d8fd.json'
    bucket = client.get_bucket(bucket)
    # buckets = client.list_buckets()
    blobs = bucket.list_blobs()
    # buckets = client.list_buckets()
    buckets_list_blobs = [item.name for item in blobs]
    # with blob.open("r") as f:
    #     data = f.read().split("\n")
    
    folder_name  = []

    all_files = ["A.txt", "B.txt", "C.csv",'D.csv']
    ['A.csv', 'A.txt', 'B.csv', 'B.txt', 'mphakashj']
    all_files = buckets_list_blobs
    # separatining the data
    list_of_all_texts_files = [] 
    list_of_all_csv_files = []

    for item in all_files:
        if item.endswith("txt"):
            list_of_all_texts_files.append(item)
        else:
            list_of_all_csv_files.append(item)


    if len(list_of_all_texts_files)>0:
        for i in list_of_all_texts_files:

            blob = bucket.blob(i)


            with blob.open("r") as f:
            # reading the file
                data = f.read()
                data_into_list = data.split("\n")
                # my_file.close()

                files_in_both_lists = []
                for q in data_into_list:
                    if q in list_of_all_csv_files:
                        files_in_both_lists.append(q)



                if len(files_in_both_lists)==len(data_into_list):
                    # timestr = datetime.now().strftime("%Y_%m_%d-%I_%M_%S_%p")
                    timestr = datetime.now().strftime("%Y_%m_%d")

                    folder_name.append(f'{timestr}_{i}')

                    a_ = [f'{i}'.split('.')[0]]

                    # copy(source_uri = f'gs://mphakashj/{i}', destination_uri = f'gs://johennes/{timestr}_{a_[0]}/')
                    move(source_uri = f'gs://mphakashj/{i}', destination_uri = f'gs://johennes/Processed_/')
                    print('files matched')
                    # passed_csv, failed_csv = validate_columns_names(data_into_list)

                    # if len(passed_csv)==len(data_into_list):

                        


                    #     print('data validation passed')

                    #     print ('run pipeline')

                    #     move(source_uri = f'gs://mphakashj/{i}', destination_uri = f'gs://johennes/{timestr}/')

                    #     for file in data_into_list:

                    #         move(source_uri = f'gs://mphakashj/{file}', destination_uri = f'gs://johennes/{timestr}/')

                else:
                    move(source_uri = f'gs://mphakashj/{i}', destination_uri = f'gs://johennes/Rejected/')
                    print('files does not match')

    else:
        print('No text file available')
    return folder_name


folder_name = file_name_validation(project='dbt-demos-370908', bucket='mphakashj')
 
validate_columns_names(folder_name)







