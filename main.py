import os
import boto3
import sys
import json
import pandas as pd
from botocore.client import Config
import io
import rosbag
import datetime


with open('sisaev_connect.json') as f:
    data_connect = json.load(f)
with open('sisaev_destination.json') as f:
    data_dest = json.load(f)

data_connect = pd.DataFrame([data_connect])
data_dest = pd.DataFrame([data_dest])

start_time = int(datetime.datetime.now().timestamp())
print(start_time)

s3 = boto3.resource(service_name='s3',
                    endpoint_url=data_connect.url[0],
                    aws_access_key_id=data_connect.accessKey[0],
                    aws_secret_access_key=data_connect.secretKey[0],
                    config=Config(signature_version=data_connect.api[0]),
                    region_name='us-east-1'
                    )

bucket_name_dest = data_dest.bucket[0]
bucket_name_in = 'test-task-01'

my_bucket_in = s3.Bucket(bucket_name_in) #бакет где лежат .bag файлы
my_bucket_out = s3.Bucket(bucket_name_dest)#бакет куда нужно сложить png

obj_list = [] #список объектов
for obj in my_bucket_in.objects.filter(Prefix='bg/'):
    if str(obj.key).__contains__('.bag'):
        obj_list.append(obj)
        print(obj.key)

#очистка содержимого бакета на выходе
for obj in my_bucket_out.objects.all():
    obj.delete()
    print(obj.key)

'''
нужно парсить кусками файл в потоке, не скачивая файл себе на локальный диск,
но непонятно как определять начало и конец самого содержимого конкретного топика в структура .bag.
Топик можно найти, но где начинается и заканчивается само сообщение неочевидно по потоку байтов.
'''
'''
#распарсить содержимое .bag файла
temp = obj_list[0].get()#['Body'] #содержимое первого бакета
print(dir(temp['Body']))
s = ''
for chunk in temp['Body'].iter_chunks(485760): #10MB - 10485760
    if (str(chunk).__contains__('/image_raw')):
        #print(str(chunk))
        #print(list(chunk))
        with open('1.txt', 'wb') as f:
            f.write(chunk)
            #print(chunk)
        break
    #print(list(chunk))
    #break
'''


def parse_files(path, out_, num, topic_in='image', time=''):
    #'bg/scenarion_id=1_subtask_id=1.bag'
    my_bucket_in.download_file(path, f'{out_}.bag')
    data = f'{out_}.bag'

    bag = rosbag.Bag(data)
    full_path = ''
    i = 1
    for topic, msg, time in bag:
        time = str(datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S-")).replace("-", '_')

        if str(topic).__contains__(topic_in):
            end_time = int(datetime.datetime.now().timestamp())
            if str(topic).__contains__('compressed'):
                full_path = f'{topic}/' \
                            f'CompressedImage/{str(start_time)}_{str(end_time)}/{time}bag{i}.png'. \
                    replace(" ", "-").replace(":", "-")
            else:
                full_path = f'{topic}/' \
                            f'Image/{str(start_time)}_{str(end_time)}/{time}bag{i}.png'. \
                    replace(" ", "-").replace(":", "-")

            filename = f'images/{time}bag{i}.png'.replace(":", '_')
            body = io.BytesIO(msg.data)
            chunk = body.read()
            with open(filename, 'wb') as f:
                f.write(chunk)
                my_bucket_out.Object(full_path).put(Body=chunk)

    end_global = int(datetime.datetime.now().timestamp())
    my_bucket_out.Object('global_time').put(Body=f'{start_time}\n{end_global}')


for i in range(0, len(obj_list)):
    path = obj_list[i].key
    parse_files(path, out_=f'test{i}', num=i)