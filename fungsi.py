import pandas as pd

# Fungsi untuk merge data
def process_merge_data(file_shipment, file_batmis, file_procurement):
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

        return dataMerge

    except Exception as e:
        raise ValueError(f"Terjadi kesalahan saat memproses merge data: {e}")

# Fungsi untuk pivot data
def process_pivot_data(dataMerge):
    try:
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
            return series

        # Konversi kolom tanggal
        dataMerge['CREATED DATE'] = convert_date(dataMerge['CREATED DATE'])
        dataMerge['RRP_DATE'] = convert_date(dataMerge['RRP_DATE'])

        dataMerge['CREATED DATE'] = pd.to_datetime(dataMerge['CREATED DATE'], format='%d-%m-%y', errors='coerce')
        dataMerge['RRP_DATE'] = pd.to_datetime(dataMerge['RRP_DATE'], format='%d-%m-%y', errors='coerce')

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

        dataMerge['RRP_Quartile'] = dataMerge['RRP_DATE'].dt.day.apply(categorize_rrp_date)
        dataMerge['Month_Year'] = dataMerge['CREATED DATE'].dt.strftime('%b-%y')
        dataMerge['Quartile_Label'] = dataMerge['Month_Year'] + '-' + dataMerge['RRP_Quartile']

        # Menghitung jumlah RRP_Quartile per bulan
        quartile_counts = dataMerge.groupby(['Month_Year', 'Quartile_Label']).size().unstack()

        return quartile_counts

    except Exception as e:
        raise ValueError(f"Terjadi kesalahan saat memproses pivot data: {e}")

