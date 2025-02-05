import streamlit as st
import pandas as pd
import numpy as np
import io
import fungsi

# Sidebar for additional information or options
st.sidebar.header("About")
st.sidebar.text("""
    Langkah-langkah:\n
        1. Upload File Shipment, BATMIS, dan Procurement di tempat yang sudah disediakan.\n
        2. Klik tombol "Submit & Process Merge Data" untuk melakukan proses penggabungan data.\n
        3. Tunggu hingga proses selesai dilakukan hingga muncul tombol "Download Hasil Merge".\n
        4. Klik tombol "Download Hasil Merge" untuk mendownload hasil penggabungan data.\n
        5. Jika ingin lanjut untuk proses pivoting data maka silakan klik tombol "Process Pivot Data".\n
        6. Tunggu hingga proses pivot selesai dan akan muncul tombol "Download Pivot Data".
""")

# Streamlit App
st.title("PO Data Processing")

# Tombol Refresh untuk menghapus session state
def reset_session():
    for key in list(st.session_state.keys()):
        del st.session_state[key]
    st.rerun()  # Menggunakan st.rerun() untuk refresh aplikasi

if st.button("🔄 Reset"):
    reset_session()

fileShipment = st.file_uploader("Upload Shipment File (Excel)", type=['xlsx'])
fileBatmis = st.file_uploader("Upload Batmis File (CSV)", type=['csv'])
fileProcurement = st.file_uploader("Upload Procurement File (Excel)", type=['xlsx'])

if fileShipment and fileBatmis and fileProcurement:
    if st.button("Submit & Process Merge Data"):
        try:
            dataMerge, oldestDate, newestDate= fungsi.process_merge_data(fileShipment, fileBatmis, fileProcurement)
            # oldestDate = dataMerge['CREATED DATE_x'].min()
            # newestDate = dataMerge['CREATED DATE_x'].max()

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                dataMerge.to_excel(writer, index=False, sheet_name='Processed Data')
            output.seek(0)
            st.session_state['oldestDate'] = oldestDate
            st.session_state['newestDate'] = newestDate
            st.session_state['processed_file'] = output
            st.session_state['dataMerge'] = dataMerge

            st.success("Data berhasil diproses! Silakan unduh hasilnya di bawah.")
        except Exception as e:
            st.error(e)

    if 'processed_file' in st.session_state:
        st.download_button(
            
            label="Download Hasil Merge",
            data=st.session_state['processed_file'],
            file_name='PROCESSED DATA_%s_%s.xlsx' %(st.session_state['oldestDate'], st.session_state['newestDate']),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    if 'dataMerge' in st.session_state and st.button("Process Pivot Data"):
        try:
            pivotCreated_RRP, pivotCreated_Shipment, pivotCreated_ShipmentQ, oldestDate2, newestDate2 = fungsi.process_pivot_data(st.session_state['dataMerge'])

            output_pivot = io.BytesIO()
            
            with pd.ExcelWriter(output_pivot, engine='xlsxwriter') as writer:
                pivotCreated_RRP.to_excel(writer, sheet_name='Timeline RRP Table', index=True)
                pivotCreated_Shipment.to_excel(writer, sheet_name='Shipment Movement (Per Date)', index=True)
                pivotCreated_ShipmentQ.to_excel(writer, sheet_name='Shipment Movement (Per Q)', index=True)

            output_pivot.seek(0)
            st.session_state['oldestDate2'] = oldestDate2
            st.session_state['newestDate2'] = newestDate2
            st.session_state['pivot_file'] = output_pivot

            st.success("Pivot Data berhasil dibuat! Silakan unduh hasilnya di bawah.")
        except Exception as e:
            st.error(e)

    if 'pivot_file' in st.session_state:
        st.download_button(
            label="Download Pivot Data",
            data=st.session_state['pivot_file'],
            file_name='TIMETABLE ORDER_%s_%s.xlsx' %(st.session_state['oldestDate2'], st.session_state['newestDate2']),
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
