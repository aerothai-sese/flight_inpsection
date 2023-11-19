from scapy.all import *
import json
import datetime
from binascii import hexlify
import asterix4py
from datetime import datetime, timezone
import pandas as pd
import time
import sys
from plotly import graph_objects as go 
import certifi
from pathlib import Path
import streamlit as st
from bson import json_util,ObjectId
from io import BytesIO
import gzip
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

import boto3
import requests 
from botocore.exceptions import ClientError

r2_endpoint = st.secrets.cloudflare.endpoint_url 
r2_access =  st.secrets.cloudflare.r2_access
r2_secret =  st.secrets.cloudflare.r2_secret

mongo_user = st.secrets.mongo.user
mongo_password = st.secrets.mongo.password

# Upload to cloudflare
def upload_file_s3(filename,bucket,object_name=None):
    if object_name is None :
        object_name = os.path.basename(filename)
    s3_client = boto3.resource('s3',
                   endpoint_url = r2_endpoint,
                   aws_access_key_id=r2_access,
                   aws_secret_access_key=r2_secret)
    try :
        response = s3_client.meta.client.upload_file(filename,bucket,object_name)
	s3_client.close()
    except ClientError as e:
        logging.error(e)
        return False
    return True

# Decode part
def flatten_json(y):
    out = {}

    def flatten(x, name=''):
        if type(x) is dict:
            for a in x:
                flatten(x[a], name + str(a) + '_')
        elif type(x) is list:
            i = 0
            for a in x:
                flatten(a, name + str(i) + '_')
                i += 1
        else:
            out[name[:-1]] = x

    flatten(y)
    return out

def save_r2_files(uploaded_files,bucket,object_name=None):
    s3_client = boto3.resource('s3',
                   endpoint_url = r2_endpoint,
                   aws_access_key_id=r2_access,
                   aws_secret_access_key=r2_secret)
    for filename in uploaded_files:
        #if object_name is None :
        #    object_name = os.path.basename(filename)
        
        try :
            s3_client.upload_fileobj(filename,bucket,filename.name)
            #response = s3_client.meta.client.upload_file(filename.name,bucket,filename)
	    s3_client.close()
        except ClientError as e:
            logging.error(e)
            
            return e
    return "Upload successful"

# save file with local
def save_local_files(uploaded_files,cs):
    files = uploaded_files
    upload_dir = cs
    Path('./upload_files/' +upload_dir).mkdir(parents=True, exist_ok=True) 
    
    if len(files) > 0 :
        for file in files:
            filename = file.name
            with open('./upload_files/'+upload_dir+'/'+ filename,'wb') as f:
                f.write(file.getvalue())

#convert time of the day to timestamp format
def ToD2Ts(packet,ToD):
    today = datetime.fromtimestamp(int(packet.time)).strftime('%Y-%m-%d')
    hms = time.strftime('%H:%M:%S', time.gmtime(ToD))
    return today + " " + hms 

def list_file(dir_path):
    res = []
    for path in os.listdir(dir_path):
        if os.path.isfile(os.path.join(dir_path,path)):
            res.append(path)
    return res

def read_pcap(path):
    cap_file = rdpcap(path)
    return cap_file

def decode_cat48_list(cap_file):
    cat48_list = []
    i = 0
    for packet in cap_file:
        hex_packet = hexlify((bytes(packet.load)))

        if hex_packet.decode('utf-8')[:2] == '30' :
            try :

                decoded = asterix4py.AsterixParser(bytes(packet[2].load))

                x = decoded.get_result()

                for j in x:
                    d = flatten_json(x[j])
                    d['140_ToD'] = ToD2Ts(packet,d['140_ToD'])
                    cat48_list.append(d)

            except:
                pass
    return cat48_list

def decode_cat21(cap_file):
    cat48_list = []
    i = 0
    for byte in cap_file:
        hex_packet = hexlify(byte)

        if hex_packet.decode('utf-8')[:2] == '30' :
            try :

                decoded = asterix4py.AsterixParser(bytes(packet[2].load))

                x = decoded.get_result()

                for j in x:
                    d = flatten_json(x[j])
                    d['140_ToD'] = ToD2Ts(packet,d['140_ToD'])
                    cat48_list.append(d)

            except:
                pass
    return cat48_list

def decode_all(upload_dir):
    data = []
    list_files = list_file('./upload_files/' + upload_dir)

    for file in list_files:
        cap_file = read_pcap("./upload_files/" +upload_dir+"/"+ file)
        data = data + decode_cat48_list(cap_file)
    return data

def decode_cat48(uploaded_files,cs,bucket):

    data = []

    for  file in uploaded_files:
        pcap_data =  gzip.decompress(file.getvalue())
        cap_file = rdpcap(BytesIO(pcap_data))
        data = data + decode_cat48_list(cap_file)
                
    return data


def get_df(data):

	df = pd.DataFrame(data)


	filter_col = [col for col in df if col.startswith('250') or col.startswith('170') or col.startswith('230') or 
	              col.startswith('170') or  col.startswith('020') or col.endswith('spare') or 
	             col.startswith('400') or col.startswith('500')] 
	filter_col.append('030_WE')
	filter_col.append('030_FX')
	filter_col.append('070_V')
	filter_col.append('070_G')
	filter_col.append('090_V')
	filter_col.append('090_G')
	filter_col.append('070_L')
	filter_col.append('SP_SP')



	df_filter = df.drop(filter_col,axis=1,errors='ignore')
	df_filter['240_TId'] = df_filter['240_TId'].str.strip()

	return df_filter


def select_flight(df,flight):
    df_filter = df.loc[df['240_TId'] ==flight]
    return df_filter

def df2json(df):
    records = df.to_dict('records')
    return records


# Mongodb connector
def insert_to_mongo(list_json,site):
    from pymongo.mongo_client import MongoClient
    from pymongo.server_api import ServerApi
	
    uri = f"mongodb+srv://{mongo_user}:{mongo_password}@cluster0.gwplkpc.mongodb.net/?retryWrites=true&w=majority"

    #uri = "mongodb+srv://art_sese_fi_2023:sese_art_2023_fi@cluster0.gwplkpc.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(uri,server_api=ServerApi('1'))

    mydb = client ['FLIGHT_INSPECTION_DB']
    mycol = mydb[site]


    insert = mycol.insert_many(list_json)
    client.close()
    return insert.inserted_ids

def get_flight():
    from pymongo.mongo_client import MongoClient
    from pymongo.server_api import ServerApi
    ca = certifi.where()
    uri = f"mongodb+srv://{mongo_user}:{mongo_password}@cluster0.gwplkpc.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(uri,server_api=ServerApi('1'))

    mydb = client ['FLIGHT_INSPECTION_DB']
    mycol = mydb["AsterixCat48"]

    flight = mycol.distinct("240_TId")
    client.close()
    return flight

def qry(tx,bucket):

    uri = f"mongodb+srv://{mongo_user}:{mongo_password}@cluster0.gwplkpc.mongodb.net/?retryWrites=true&w=majority"
    client = MongoClient(uri,server_api=ServerApi('1'))

    mydb = client ['FLIGHT_INSPECTION_DB']
    mycol = mydb[bucket]

    object_ids_to_search =[ ObjectId(i) for i in tx]
    result = mycol.find({"_id":{"$in": object_ids_to_search}})

    doc_list = list(result)
    df = pd.DataFrame(doc_list)
    return df

def json2df(recs):
    df = pd.DataFrame(list(recs))
    return df

def tx_to_list(tx):
    tx_=[]
    for i in tx:
        tx_.append(str(i))
    return tx_

def check_existing_meta():
    return None

def create_meta(df,site,tx):
    meta ={}
    dof = dof = df.iloc[0]["140_ToD"].split()[0]
    start_time = df.iloc[0]["140_ToD"].split()[1]
    end_time = df.iloc[-1]["140_ToD"].split()[1]
    cs = df.iloc[0]["240_TId"].strip()
    tx_ = tx_to_list(tx)
    meta[site] = [dof,start_time,end_time,cs,tx_]
    return meta

# metadata.json structure {"uploaded_metadata : [{"dof":...,"start_time":...,"end_time":....} 
#   ,"cs": ...,"tx":[...] ,...]}
def write_meta(meta,key):
    s3_client = boto3.resource('s3',
	   endpoint_url = r2_endpoint,
	   aws_access_key_id=r2_access,
	   aws_secret_access_key=r2_secret)
    try:

        response = s3_client.get_object(Bucket='etc', Key=key)
        json_content = response['Body'].read()
        metadata = json.loads(json_content)

        #metadata = {}
        #metadata["uploaded_metadata"] =[]

        metadata["uploaded_metadata"].append(meta)
        
        json_string = json.dumps(metadata)
        json_file = io.BytesIO(json_string.encode('utf-8'))  
        s3_client.upload_fileobj(json_file,'etc',key)
        s3_client.close()
    except s3_client.exceptions.NoSuchKey:
        print(f"The specified object with key does not exist in the bucket.")
    return None

def read_meta(cat):
    s3_client = boto3.resource('s3',
	   endpoint_url = r2_endpoint,
	   aws_access_key_id=r2_access,
	   aws_secret_access_key=r2_secret)
    try:
    # Use the get_object method to retrieve the object by name

        if cat =="ADSB":
            key ='metadata_adsb'
        else:
            key ="metadata_ssr"
        
        response = s3_client.get_object(Bucket='etc', Key=key)
	s3_client.close()
        # Access the object's content
        object_content = response['Body'].read()
        return json.loads(object_content.decode('utf-8'))
        # You can now work with the content as needed
    
    except s3_client.exceptions.NoSuchKey:
        print(f"Metadata in etc bucket is missing.")

    
#plot part
def plot_3d(df):
    x = df['042_X']
    y = df['042_Y']
    z = df['090_FL']

    sensor = df['010_SAC'].iloc[0]

    fig = go.Figure(data=go.Scatter3d(
        x=x,y=y,z=z,
        marker=dict(size=1,color=z)
        ,
        line = dict(
            color='darkblue',
            width=0.1
            )
        )
    )

    fig.add_trace(go.Scatter3d(x=[0],y=[0],z=[0],text="ssr",
        mode='markers'))

    #vordme point
    #fig.add_trace(go.)

    fig.update_layout(scene=dict(aspectratio=dict(x=1, y=1, z=1)),height=1000,width=1000)
    fig.update_yaxes(
        scaleanchor = "x",
        scaleratio = 1,
      )
    fig.update_xaxes(
        scaleanchor = "y",
        scaleratio = 1,
      )
    return fig

def get_max_value(df,height):
    df_filter = df[(df['090_FL'] >= height-1) & (df['090_FL'] <= height +1 )]
    max_index = df_filter['040_RHO'].idxmax()
    return df.loc[max_index]

def get_min_value(df,height):
    df_filter = df[(df['090_FL'] >= height-1) & (df['090_FL'] <= height +1 )]
    min_index = df_filter['040_RHO'].idxmin()
    return df.loc[min_index]

def get_pattern(df):
    ft = [10,20,30,50,70,100,150,200]
    pattern_max,pattern_min = [],[]
    for i in ft:
        pattern_max.append(get_max_value(df,i).to_dict())
        pattern_min.append(get_min_value(df,i).to_dict())
    return pattern_max,pattern_min

def get_bound(pattern_max,pattern_min):
    x_max = [i['040_RHO'] for i in pattern_max]
    x_max.insert(0, 0)
    y_max = [i['090_FL'] for i in pattern_max]
    y_max.insert(0, 0)

    x_min = [i['040_RHO'] for i in pattern_min]
    x_min.insert(0, 0)
    y_min = [i['090_FL'] for i in pattern_min]
    y_min.insert(0, 0)
    return x_max,y_max,x_min,y_min
def esttimate(x_max,y_max,x_min,y_min):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=x_max,y=y_max,line=dict(width=0.5),fill=None))
    fig.add_trace(go.Scatter(x=x_min,y=y_min,line=dict(width=0.5),fill='tonexty'))

    fig.layout.yaxis.scaleanchor="x"
    return fig
