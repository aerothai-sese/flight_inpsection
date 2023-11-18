import streamlit as st
import datetime as dt
from fi_utils import *
import pandas as pd 

st.header("Compare between surveillance site.")

cat = st.selectbox('Please select asterix type',['ADSB', 'RADAR'])
if cat =='ADSB':
	#bucket = 'adsb' + site.lower()
	st.write("ADSB")
else:
	st.write("SSR")
	#bucket = 'ssr' + site.lower()

metadata = read_meta(cat)
data = metadata["uploaded_metadata"]
select_meta = {}

for i,k in enumerate(data):
	for j in k:
		fmt = f"Datasets of {k[j][3]} has been flown on {k[j][0]} {k[j][1]} to {k[j][2]}"
		select_meta[i] = fmt


dataset1 = st.selectbox("Select 1st datasets.",options=list(select_meta.keys()),format_func=lambda x: select_meta[x])
dataset2 = st.selectbox("Select 2nd datasets.",options=list(select_meta.keys()),format_func=lambda x: select_meta[x])
st.write(list(data[dataset1].values())[0][4])

if dataset1 :
	tx1 = data[dataset1][site][4]

	df1 = qry(tx1,bucket)
	df1['140_ToD'] = pd.to_datetime(df1['140_ToD'])
	df1 = df1.sort_values(by='140_ToD')
