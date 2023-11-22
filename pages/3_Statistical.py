import streamlit as st
import datetime as dt
from fi_utils import *
import pandas as pd 

st.header("Compare between surveillance site.")


cat = st.selectbox('Please select asterix type',['ADSB', 'RADAR'])
if cat =='ADSB':
	site = st.selectbox('Please select ADSB site',['MK', 'HY','UB','SM','IN','UD'])
	bucket = 'adsb' + site.lower()
else:
	site = st.selectbox('Please select Surveillance site',['DMA', 'SBA','CMA',
		'SRT','HHN','PSL','PUT','UBN','HTY','CMP','CTR','ROT','UDN','INT','PHK'])
	bucket = 'ssr' + site.lower()


metadata = read_meta(cat)
data = metadata["uploaded_metadata"]
select_meta = {}

for i,k in enumerate(data):
	for j in k:
		if j == site:
			fmt = f"Datasets of {k[j][3]} has flown on {k[j][0]} {k[j][1]} to {k[j][2]}"
			select_meta[i] = fmt


dataset1 = st.selectbox("Select 1st datasets.",options=list(select_meta.keys()),format_func=lambda x: select_meta[x])
dataset2 = st.selectbox("Select 2nd datasets.",options=list(select_meta.keys()),format_func=lambda x: select_meta[x])
if dataset1 and dataset2:
	tx1 = list(data[dataset1].values())[0][4]
	tx2 = list(data[dataset2].values())[0][4]


	df1 = qry(tx1,bucket)
	df1['140_ToD'] = pd.to_datetime(df1['140_ToD'])
	df1 = df1.sort_values(by='140_ToD')

	pattern_max1,pattern_min1 = get_pattern(df1,site)
	x_max1,y_max1,x_min1,y_min1 = get_bound(pattern_max1,pattern_min1)


	df2 = qry(tx2,bucket)
	df2['140_ToD'] = pd.to_datetime(df2['140_ToD'])
	df2 = df2.sort_values(by='140_ToD')

	pattern_max2,pattern_min2 = get_pattern(df2,site)
	x_max2,y_max2,x_min2,y_min2 = get_bound(pattern_max2,pattern_min2)

	df_1 = pd.DataFrame({'Site I : Outer Fringe': x_max1,'Site I : Inner Fringe':x_min1})
	df_2 = pd.DataFrame({'Site II : Outer Fringe': x_max2,'Site II : Inner Fringe':x_min2})
	
	df_1 = df_1.set_axis(y_max1)
	df_2 = df_2.set_axis(y_max2)
	col1,col2 = st.columns(2)
	with col1:
		st.dataframe(df_1)
	with col2:
		st.dataframe(df_2)
