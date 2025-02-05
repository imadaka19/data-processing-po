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
        3. Tunggu hingga prosess selesai dilakukan hingga muncul tombol "Download Hasil Merge".\n
        4. Klik tombol "Download Hasil Merge" untuk mendownload hasil penggabungan data.\n
        5. Jika ingin lanjut untuk process pivoting data maka silahkan klik tombol "Process Pivot Data".\n
        6. Tunggu hingga process pivot selesai dan akan muncul tombol "Download Pivot Data".
""")

# Streamlit App
st.title("PO Data Processing")

file_shipment = st.file_uploader("Upload Shipment File (Excel)", type=['xlsx'])
file_batmis = st.file_uploader("Upload Batmis File (CSV)", type=['csv'])
file_procurement = st.file_uploader("Upload Procurement File (Excel)", type=['xlsx'])

if file_shipment and file_batmis and file_procurement:
    if st.button("Submit & Process Merge Data"):
        try:
            dataMerge = fungsi.process_merge_data(file_shipment, file_batmis, file_procurement)

            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                dataMerge.to_excel(writer, index=False, sheet_name='Processed Data')
            output.seek(0)

            st.session_state['processed_file'] = output
            st.session_state['dataMerge'] = dataMerge

            st.success("Data berhasil diproses! Silakan unduh hasilnya di bawah.")
        except Exception as e:
            st.error(e)

    if 'processed_file' in st.session_state:
        st.download_button(
            label="Download Hasil Merge",
            data=st.session_state['processed_file'],
            file_name="dataMerge.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    if 'dataMerge' in st.session_state and st.button("Process Pivot Data"):
        try:
            quartile_counts = fungsi.process_pivot_data(st.session_state['dataMerge'])

            output_pivot = io.BytesIO()
            with pd.ExcelWriter(output_pivot, engine='xlsxwriter') as writer:
                quartile_counts.to_excel(writer, sheet_name='Pivot Data')
            output_pivot.seek(0)

            st.session_state['pivot_file'] = output_pivot

            st.success("Pivot Data berhasil dibuat! Silakan unduh hasilnya di bawah.")
        except Exception as e:
            st.error(e)

    if 'pivot_file' in st.session_state:
        st.download_button(
            label="Download Pivot Data",
            data=st.session_state['pivot_file'],
            file_name="quartile_counts_pivot.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
