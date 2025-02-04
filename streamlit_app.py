import streamlit as st
import pandas as pd
import numpy as np
import io

# st.set_option('server.maxUploadSize', 1024)

st.title("PO Data Processing")

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

# Upload file Shipment
file_shipment = st.file_uploader("Upload Shipment File (Excel)", type=['xlsx'])

# Upload file Batmis
file_batmis = st.file_uploader("Upload Batmis File (CSV)", type=['csv'])

# Upload file Procurement
file_procurement = st.file_uploader("Upload Procurement File (Excel)", type=['xlsx'])

# Pastikan semua file diunggah sebelum menampilkan tombol submit
if file_shipment and file_batmis and file_procurement:
    if st.button("Submit & Process Merge Data"):
        try:
            # Membaca file Shipment
            dataShipmentRaw_1 = pd.read_excel(file_shipment, sheet_name='KUL-VENDOR 2025', skiprows=2)
            dataShipmentRaw_2 = pd.read_excel(file_shipment, sheet_name='BTH-VENDOR', skiprows=2)

            # Membaca file Batmis
            dataBatmisRaw = pd.read_csv(file_batmis)

            # Membaca file Procurement
            sheets = ['AFM', 'CMA', 'PPM', 'PO', 'TOOLS', 'FAST MOVING']
            dataProcurementRaw = pd.concat([pd.read_excel(file_procurement, sheet_name=sn) for sn in sheets])

            # Konversi LINE ke numerik
            dataProcurementRaw['LINE'] = pd.to_numeric(dataProcurementRaw['LINE'], errors='coerce').astype('Int64')

            # Mengolah data Shipment
            dataShipmentRaw = pd.concat([dataShipmentRaw_1, dataShipmentRaw_2])
            dataShipment = dataShipmentRaw[['ORDER TYPE', 'ORDER NUMBER', 'PN', 'SN', 'DELIVERY DATE', 'AWB/BL NUMBER']]
            dataShipment['ORDER_TYPE-NUMBER-PN'] = dataShipment['ORDER TYPE'] + '-' + dataShipment['ORDER NUMBER'].astype(str) + '-' + dataShipment['PN'].astype(str)
            dataShipment.set_index('ORDER_TYPE-NUMBER-PN', inplace=True)

            # Mengolah data Batmis
            dataBatmisRaw['ORDER_TYPE-NUMBER-PN'] = dataBatmisRaw['ORDER TYPE'] + '-' + dataBatmisRaw['ORDER NUMBER'].astype(str) + '-' + dataBatmisRaw['ORDER PN '].astype(str)
            dataBatmisRaw.set_index('ORDER_TYPE-NUMBER-PN', inplace=True)

            # Merge data Shipment & Batmis
            dataMerge = dataBatmisRaw.merge(dataShipment, how='left', left_index=True, right_index=True)

            # Menyimpan file hasil ke dalam memori (BytesIO)
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                dataMerge.to_excel(writer, index=False, sheet_name='Processed Data')
            output.seek(0)

            # Simpan hasil ke session_state untuk digunakan kembali
            st.session_state['processed_file'] = output
            st.session_state['dataMerge'] = dataMerge  # Simpan hasil merge untuk pivot

            st.success("Data berhasil diproses! Silakan unduh hasilnya di bawah.")
        except Exception as e:
            st.error(f"Terjadi kesalahan saat memproses data: {e}")

# Menampilkan tombol download jika file telah diproses
if 'processed_file' in st.session_state:
    st.download_button(
        label="Download Hasil Merge",
        data=st.session_state['processed_file'],
        file_name="dataMerge.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

# **Tombol Pivot Data**
if 'dataMerge' in st.session_state:
    if st.button("Process Pivot Data"):
        try:
            df = st.session_state['dataMerge']

            # Daftar kemungkinan format tanggal
            possible_date_formats = ['%d-%b-%y', '%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%d-%m-%Y']

            def convert_date(series):
                for fmt in possible_date_formats:
                    try:
                        converted = pd.to_datetime(series, format=fmt, errors='coerce')
                        if not converted.isna().all():
                            return converted.dt.strftime('%d-%m-%y')
                    except Exception:
                        continue
                return series  # Jika semua format gagal

            # Konversi kolom tanggal
            df['CREATED DATE'] = convert_date(df['CREATED DATE'])
            df['RRP_DATE'] = convert_date(df['RRP_DATE'])

            df['CREATED DATE'] = pd.to_datetime(df['CREATED DATE'], format='%d-%m-%y', errors='coerce')
            df['RRP_DATE'] = pd.to_datetime(df['RRP_DATE'], format='%d-%m-%y', errors='coerce')

            # Menambahkan kolom kuartil untuk RRP_DATE
            def categorize_rrp_date(day):
                if pd.isna(day):
                    return None
                elif 1 <= day <= 10:
                    return 'Q1'
                elif 11 <= day <= 20:
                    return 'Q2'
                else:
                    return 'Q3'

            df['RRP_Quartile'] = df['RRP_DATE'].dt.day.apply(categorize_rrp_date)
            df['Month_Year'] = df['CREATED DATE'].dt.strftime('%b-%y')
            df['Quartile_Label'] = df['Month_Year'] + '-' + df['RRP_Quartile']

            # Menghitung jumlah RRP_Quartile per bulan
            quartile_counts = df.groupby(['Month_Year', 'Quartile_Label']).size().unstack()

            # Simpan hasil pivot ke BytesIO untuk diunduh
            output_pivot = io.BytesIO()
            with pd.ExcelWriter(output_pivot, engine='xlsxwriter') as writer:
                quartile_counts.to_excel(writer, sheet_name='Pivot Data')
            output_pivot.seek(0)

            # Simpan hasil pivot di session_state
            st.session_state['pivot_file'] = output_pivot

            st.success("Pivot Data berhasil dibuat! Silakan unduh hasilnya di bawah.")
        except Exception as e:
            st.error(f"Terjadi kesalahan saat membuat pivot: {e}")

# **Tombol Download Pivot Data**
if 'pivot_file' in st.session_state:
    st.download_button(
        label="Download Pivot Data",
        data=st.session_state['pivot_file'],
        file_name="quartile_counts_pivot.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
