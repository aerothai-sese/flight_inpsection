import streamlit as st
import datetime as dt
from fi_utils import *
import pandas as pd 


st.header("Visualize")

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
			fmt = f"Datasets of {k[j][3]} has been flown on {k[j][0]} {k[j][1]} to {k[j][2]}"
			select_meta[i] = fmt
		
selected_fi = st.selectbox("Select chkecked flight.",options=list(select_meta.keys()),format_func=lambda x: select_meta[x])

if selected_fi:
	tx = data[selected_fi][site][4]

	df = qry(tx,bucket)
	df['140_ToD'] = pd.to_datetime(df['140_ToD'])
	df = df.sort_values(by='140_ToD')

	with st.expander("Dataset"):
		st.dataframe(df.head(200))

	fig1 = plot_3d(df)

	col1,col2 = st.columns(2)
	
	with col1:
		viz = st.button("Visualize !") 
	with col2:
		delete = st.button("Delete datasets") 
		st.error("Warning : This datasets will be deleted.")

	if viz :
		st.header('Historical flight inspection plot', divider='rainbow')
		st.plotly_chart(fig1,use_container_width=True)
		st.header('Estimation of the radiation pattern', divider='rainbow')
		
		pattern_max,pattern_min = get_pattern(df,site)
		x_max,y_max,x_min,y_min = get_bound(pattern_max,pattern_min)
		fig2 = esttimate(x_max,y_max,x_min,y_min)
		st.plotly_chart(fig2,use_container_width=True)

