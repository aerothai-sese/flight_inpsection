import streamlit as st

st.set_page_config(
    page_title="Flight inspection visualization",
    page_icon="🛩️",
)
st.title('Flight inspection visualization 🛩️')
st.subheader("What is Flight inspection visualization ?")
st.markdown("""FIV is the light weight 3D data visualization of flight inspection of Surveillance system and historical data.
	""")

st.divider()

st.subheader("How to ?")
st.markdown(""" - New data.
	1. Select upload on the sidebar.
	2. Drag pcap file(s) to the upload widget.
	3. Press decode button.
	4. Select specific callsign and insert to database.
	5. Select 3D Visualization to analytic your flight.""")


st.markdown("""- Already have data.
	1. Select 3D Visualization to analytic your flight. """) 