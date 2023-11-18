import streamlit as st
from fi_utils import *
import pathlib
import pandas as pd 

st.set_page_config(
    page_title="Upload data",
    page_icon="",
)

st.header("Update data.")
st.markdown("Upload pcap file(s) below there.")

uploaded_files = st.file_uploader("Choose pcap(s) file.",
	accept_multiple_files = True,
	type=['.gz','pcap']
	)

cs = st.text_input(label="Callsign of inspection flight")
cat = st.selectbox('Please select asterix type',['CAT21', 'CAT48'])
if cat =='CAT21':
	site = st.selectbox('Please select ADSB site',['MK', 'HY','UB','SM','IN','UD'])
	bucket = 'adsb' + site.lower()
else:
	site = st.selectbox('Please select Surveillance site',['DMA', 'SBA','CMA',
		'SRT','HHN','PSL','PUT','UBN','HTY','CMP','CTR','ROT','UDN','INT','PHK'])
	bucket = 'ssr' + site.lower()



if cs != "" and len(uploaded_files) != 0: 

	submit_button = st.button('Submit')
	if submit_button :
		if cat=='CAT21':
			data = decode_cat48(uploaded_files,cs,bucket)
			key ='metadata_adsb'
		else:
			data = decode_cat48(uploaded_files,cs,bucket)
			key ="metadata_ssr"
		# Test flight is TVJ130
		df_filter = get_df(data)
		
		df_flight = select_flight(df_filter,cs)

		st.write("Example 5 records data.")
		st.dataframe(df_flight.head(5))	

		
		#write_meta(site)

		records= df2json(df_flight)
		
		response = save_r2_files(uploaded_files,bucket,object_name=None)
		st.write(response)
			
		try :
			tx = insert_to_mongo(records,bucket)
			st.write(f"Dataframe with {len(records)} records is inserted to mongodb.")
			
			meta = create_meta(df_flight,site,tx)
			

			metadata = write_meta(meta,key)
			#st.write(metadata)

		except Exception as e:
			print(e)