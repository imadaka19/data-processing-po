import pandas as pd
from datetime import datetime
import numpy as np
import re

# def detect_delimiter(file, encodings=['utf-8', 'ISO-8859-1', 'latin1']):
#     """Deteksi delimiter dengan membaca beberapa baris pertama dari file CSV."""
#     for enc in encodings:
#         try:
#             with open(file, 'r', encoding=enc) as f:
#                 first_line = f.readline()
#                 if ';' in first_line and ',' in first_line:
#                     return ';'  # Default ke ";" jika keduanya ada
#                 elif ';' in first_line:
#                     return ';'
#                 else:
#                     return ','
#         except UnicodeDecodeError:
#             print(f"Error decoding CSV file with {enc} encoding, trying next encoding.")
#         except FileNotFoundError:
#             print(f"Error: File {file} not found.")
#             break
#     return ','  # Default delimiter

# Format tanggal yang didukung
# date_formats = [
#     "%d-%m-%Y",  # 10-02-2024
#     # "%Y-%m-%d",  # 2024-02-10
#     # "%Y/%m/%d",  # 2024/02/10
#     # "%m/%d/%Y",  # 02/10/2024
#     "%d/%m/%Y",  # 10/02/2024
#     # "%B %d, %Y", # February 10, 2024
#     "%d-%b-%y",  # 10-Feb-24
#     "%d-%m-%y",  # 10-02-24
#     "%d/%b/%y",  # 10-Feb-24
#     "%d/%m/%y",  # 10-02-24
# ]

# def convert_date(date_string, formats=date_formats, target_format="%Y-%m-%d"):
#     # Cek jika nilai None, NaN, atau bukan string yang bisa diproses
#     if pd.isna(date_string) or str(date_string).lower() == 'nan' or date_string is None:
#         return np.nan  # Kembalikan NaN agar tetap konsisten dalam DataFrame
    
#     # Loop melalui berbagai format yang didukung
#     for fmt in formats:
#         try:
#             # Coba parsing tanggal dengan format yang tersedia
#             date_obj = datetime.strptime(str(date_string), fmt)
#             # Kembalikan tanggal dalam format target
#             return date_obj.strftime(target_format)
#         except ValueError:
#             continue  # Jika gagal, coba format berikutnya
    
#     # Jika bukan tanggal valid, ubah menjadi NaN
#     return date_string


# Fungsi untuk merge data
def process_merge_data(fileShipment, fileBatmis, fileProcurement):
    # delimiter = detect_delimiter(fileBatmis, encodings=['utf-8', 'ISO-8859-1', 'latin1'])
    try:
        # Read Data Shipment & BATMIS
        dataShipmentRaw_1 = pd.read_excel(fileShipment, sheet_name='KUL-VENDOR 2025', skiprows=2)
        dataShipmentRaw_2 = pd.read_excel(fileShipment, sheet_name='BTH-VENDOR', skiprows=2)
        dataShipmentRaw_3 = pd.read_excel(fileShipment, sheet_name='PLB MONITORING')

        dataShipmentRaw = pd.concat([dataShipmentRaw_1, dataShipmentRaw_2])

        dataBatmisRaw = pd.read_csv(fileBatmis, on_bad_lines='skip', quoting=3, delimiter=";")

        # Preparasi Data Procurement
        dataProcurementRaw_1 = pd.read_excel(fileProcurement, sheet_name='AFM')
        dataProcurementRaw_2 = pd.read_excel(fileProcurement, sheet_name='CMA')
        dataProcurementRaw_3 = pd.read_excel(fileProcurement, sheet_name='PPM')
        dataProcurementRaw_4 = pd.read_excel(fileProcurement, sheet_name='PO')
        dataProcurementRaw_5 = pd.read_excel(fileProcurement, sheet_name='TOOLS')
        dataProcurementRaw_6 = pd.read_excel(fileProcurement, sheet_name='FAST MOVING')

        dataProcurementRaw_4.rename({'ORDER NUMBER':'ORDER', 'PN DESCRIPTION':'DESCRIPTION', 'STANDARD STATUS ORDER':'STANDARD STATUS', 'CURRENCY':'CURR'}, axis=1, inplace=True)
        dataProcurementRaw_5.rename({'ORDER NUMBER':'ORDER', 'PN DESCRIPTION':'DESCRIPTION', 'STANDARD STATUS ORDER':'STANDARD STATUS', 'CURRENCY':'CURR'}, axis=1, inplace=True)
        dataProcurementRaw_6.rename({'ORDER NUMBER':'ORDER', 'PN DESCRIPTION':'DESCRIPTION', 'STANDARD STATUS ORDER':'STANDARD STATUS', 'CURRENCY':'CURR'}, axis=1, inplace=True)

        # Merging Data Procurement (6 Sheets) menjadi 1
        dataProcurementRaw = pd.concat([dataProcurementRaw_1, dataProcurementRaw_2, dataProcurementRaw_3, dataProcurementRaw_4, dataProcurementRaw_5, dataProcurementRaw_6])

        dataProcurementRaw['LINE'] = pd.to_numeric(dataProcurementRaw['LINE'], errors='coerce').astype('Int64')

        # Remove quotes from the header if they appear at both ends
        dataBatmisRaw.columns = dataBatmisRaw.columns.map(lambda x: re.sub('^"(.*)"$', r'\1', x))

        # Remove quotes from the data if they appear at both ends
        # Also remove excessive internal quotes (like "" -> ")
        dataBatmisRaw = dataBatmisRaw.applymap(lambda x: re.sub(r'^"(.*)"$', r'\1', re.sub(r'""+', '"', x)) if isinstance(x, str) else x)

        # Pengolahan data BATMIS
        #dataBatmisProcessed = dataBatmisRaw[['REQUISITION', 'ORDER TYPE', 'ORDER NUMBER', 'ORDER LINE', 'STATUS', 'CREATED DATE', 'DATE AWB OUT', 'AUTHORIZATION_DATE', 'AUTHRQ_DATE', 'AUTHRQ_ID', 'AUTHRQ_BY', 'ORDER PN', 'PN DESCRIPTION', 'GRB_HISTORY', 'QTY', 'QTY_RCVD', 'UOM', 'AWB IN NUMBER', 'RRP_DATE', 'RRP_BY', 'NAME_RRPBY']]
        dataBatmisProcessed = dataBatmisRaw[['REQUISITION', 'ORDER TYPE', 'ORDER NUMBER', 'ORDER LINE', 'STATUS', 'CREATED DATE', 'DATE AWB OUT', 'AUTHORIZATION_DATE', 'AUTHRQ_DATE', 'AUTHRQ_ID', 'AUTHRQ_BY', 'ORDER PN', 'PN DESCRIPTION', 'GRB_HISTORY', 'QTY', 'QTY_RCVD', 'UOM', 'AWB IN NUMBER', 'RRP_DATE', 'RRP_BY', 'NAME_RRPBY']]

        dataBatmisProcessed['ORDER_TYPE-NUMBER-LINE'] = dataBatmisProcessed['ORDER TYPE'] + '-' + dataBatmisProcessed['ORDER NUMBER'].astype(str) + '-' + dataBatmisProcessed['ORDER LINE'].astype(str)
        dataBatmisProcessed['ORDER_TYPE-NUMBER-PN'] = dataBatmisProcessed['ORDER TYPE'] + '-' + dataBatmisProcessed['ORDER NUMBER'].astype(str)+ '-' + dataBatmisProcessed['ORDER PN'].astype(str)
        
        dataBatmisProcessed = dataBatmisProcessed.set_index('ORDER_TYPE-NUMBER-LINE')
        dataBatmisProcessed['ORDER_TYPE-NUMBER-LINE'] = dataBatmisProcessed['ORDER TYPE'] + '-' + dataBatmisProcessed['ORDER NUMBER'].astype(str) + '-' + dataBatmisProcessed['ORDER LINE'].astype(str)

        # Pengolahan data Procurement
        dataProcurement = dataProcurementRaw[['TYPE', 'ORDER', 'LINE', 'ORDER CREATED DATE', 'ETA', 'STANDARD STATUS', 'GENERAL STATUS', 'PN']]
        dataProcurement['ORDER_TYPE-NUMBER-LINE'] = dataProcurement['TYPE'] + '-' + dataProcurement['ORDER'].astype(str) + '-' + dataProcurement['LINE'].astype(str)
        #dataProcurement['ORDER_TYPE-NUMBER-PN'] = dataProcurement['TYPE'] + '-' + dataProcurement['ORDER'].astype(str)+ '-' + dataProcurement['PN'].astype(str)

        dataProcurement = dataProcurement.set_index('ORDER_TYPE-NUMBER-LINE')
        dataProcurement['ORDER_TYPE-NUMBER-LINE'] = dataProcurement['TYPE'] + '-' + dataProcurement['ORDER'].astype(str) + '-' + dataProcurement['LINE'].astype(str)

        dataProcurement = dataProcurement.rename({'TYPE':'ORDER TYPE', 'ORDER':'ORDER NUMBER', 'LINE':'ORDER LINE', 'ORDER CREATED DATE':'CREATED DATE'}, axis=1)
        dataProcurement.drop(columns=['PN'], inplace=True)

        dataProcurement['CREATED DATE'] = pd.to_datetime(dataProcurement['CREATED DATE'], errors='coerce', format='%Y-%m-%d')

        dataProcurement['ETA'] = pd.to_datetime(dataProcurement['ETA'], errors='coerce', format='%Y-%m-%d %H:%M:%S')
        dataProcurement['ETA'] = dataProcurement['ETA'].dt.strftime('%Y-%m-%d')

        dataProcurement.rename({'TYPE':'ORDER TYPE', 'ORDER':'ORDER NUMBER', 'LINE':'ORDER LINE', 'ORDER CREATED DATE':'CREATED DATE', 'PN':'ORDER PN'}, axis=1, inplace=True)

        # Pengolahan data Shipment
        # Mengolah data Shipment Tab PLB Monitoring
        dataShipmentRaw_4 = dataShipmentRaw_3[['ORDER TYPE', 'ORDER NUMBER', 'PN', 'AWB', 'PICK UP DATE', 'PART STATUS']]
        dataShipmentRaw_4 = dataShipmentRaw_4.rename({'AWB':'AWB/BL NUMBER', 'PICK UP DATE':'DELIVERY DATE', 'PART STATUS':'STATUS NEW'}, axis=1)
        dataShipmentRaw_4['ORDER_TYPE-NUMBER-PN'] = dataShipmentRaw_4['ORDER TYPE'] + '-' + dataShipmentRaw_4['ORDER NUMBER'].astype(str)+ '-' + dataShipmentRaw_4['PN'].astype(str)
        dataShipmentRaw_4.set_index(['ORDER_TYPE-NUMBER-PN'], inplace=True)
        dataShipmentRaw_4['ORDER_TYPE-NUMBER-PN'] = dataShipmentRaw_4['ORDER TYPE'] + '-' + dataShipmentRaw_4['ORDER NUMBER'].astype(str)+ '-' + dataShipmentRaw_4['PN'].astype(str)

        #dataShipmentRaw_4['DELIVERY DATE'] = pd.to_datetime(dataShipmentRaw_4['DELIVERY DATE'], format="%d/%m/%Y", errors="ignore").astype('str')

        
        def swap_day_month(date):
            if isinstance(date, datetime):
                # Swap day and month
                return datetime(date.year, date.day, date.month, date.hour, date.minute, date.second)
            return date  # Return the string unchanged

        swapped_data = [swap_day_month(d) for d in dataShipmentRaw_4['DELIVERY DATE']]

        dataShipmentRaw_4['DELIVERY DATE'] = swapped_data


        dataShipmentRaw_4['DELIVERY DATE'] = pd.to_datetime(dataShipmentRaw_4['DELIVERY DATE'], errors="coerce", dayfirst=False)
        dataShipmentRaw_4['DELIVERY DATE'] = dataShipmentRaw_4['DELIVERY DATE'].dt.strftime('%Y-%m-%d')
        dataShipmentRaw_4['DELIVERY DATE']= pd.to_datetime(dataShipmentRaw_4['DELIVERY DATE'], errors='ignore')

        #dataShipmentRaw_4['DELIVERY DATE'] = pd.to_datetime(dataShipmentRaw_4['DELIVERY DATE'], errors='coerce', format='%d/%m/%Y')
        #dataShipmentRaw_4['DELIVERY DATE'] = pd.to_datetime(dataShipmentRaw_4['DELIVERY DATE'], errors='coerce')
         # Convert to datetime, handle errors
        #dataShipmentRaw_4['DELIVERY DATE'] = dataShipmentRaw_4['DELIVERY DATE'].dt.strftime('%Y-%m-%d')

        # Mengolah data ShipmentRaw
        dataShipmentRaw = dataShipmentRaw[['ORDER TYPE', 'ORDER NUMBER', 'PN', 'AWB/BL NUMBER', 'DELIVERY DATE', 'STATUS NEW']]
        dataShipmentRaw['ORDER_TYPE-NUMBER-PN'] = dataShipmentRaw['ORDER TYPE'] + '-' + dataShipmentRaw['ORDER NUMBER'].astype(str)+ '-' + dataShipmentRaw['PN'].astype(str)
        dataShipmentRaw.set_index(['ORDER_TYPE-NUMBER-PN'], inplace=True)
        dataShipmentRaw['ORDER_TYPE-NUMBER-PN'] = dataShipmentRaw['ORDER TYPE'] + '-' + dataShipmentRaw['ORDER NUMBER'].astype(str)+ '-' + dataShipmentRaw['PN'].astype(str)

        dataShipment = pd.concat([dataShipmentRaw, dataShipmentRaw_4])

        # Mengolah data Shipment Merged
        dataShipment.rename({'STATUS NEW':'STATUS', 'DELIVERY DATE':'DATE AWB OUT'}, axis=1, inplace=True)
        dataShipment2 = dataShipment
        dataShipment2 = dataShipment2[['ORDER TYPE', 'ORDER NUMBER', 'PN', 'DATE AWB OUT', 'AWB/BL NUMBER', 'STATUS']]
        dataShipment2['ORDER_TYPE-NUMBER-PN'] = dataShipment2['ORDER TYPE'] + '-' + dataShipment2['ORDER NUMBER'].astype(str)+ '-' + dataShipment2['PN'].astype(str)
        dataShipment2 = dataShipment2.set_index('ORDER_TYPE-NUMBER-PN')
        dataShipment2['ORDER_TYPE-NUMBER-PN'] = dataShipment2['ORDER TYPE'] + '-' + dataShipment2['ORDER NUMBER'].astype(str)+ '-' + dataShipment2['PN'].astype(str)

        # Merging Data BATMIS & Procurement
        dataMerge = dataBatmisProcessed.merge(dataProcurement, how='left', left_index=True, right_index=True)
        dataMerge.reset_index(inplace=True)
        dataMerge.set_index('ORDER_TYPE-NUMBER-PN', inplace=True)
        dataMerge['ORDER_TYPE-NUMBER-PN'] = dataMerge['ORDER TYPE_x'] + '-' + dataMerge['ORDER NUMBER_x'].astype(str)+ '-' + dataMerge['ORDER PN'].astype(str)

        # Merging DataMerge dengan data Shipment
        dataMergeAll = dataMerge.merge(dataShipment2, how='left', left_index=True, right_index=True)

        dataMergeAll['DATE AWB OUT_x'] = dataMergeAll['DATE AWB OUT_y'].fillna(dataMergeAll['DATE AWB OUT_x'])
        dataMergeAll['AWB IN NUMBER'] = dataMergeAll['AWB/BL NUMBER'].fillna(dataMergeAll['AWB IN NUMBER'])
        # Pengolahan data MergeAllFiltered dan export data Merged

        dataMergeAllFiltered = dataMergeAll[[
             'ORDER_TYPE-NUMBER-LINE', 'REQUISITION', 'ORDER TYPE_x', 'ORDER NUMBER_x', 'ORDER LINE_x', 'STATUS_x', 'CREATED DATE_x',
             'DATE AWB OUT_x', 'AUTHORIZATION_DATE', 'AUTHRQ_DATE', 'AUTHRQ_BY', 'ORDER PN', 'PN DESCRIPTION', 'GRB_HISTORY',
             'QTY', 'QTY_RCVD', 'UOM', 'AWB IN NUMBER', 'AWB/BL NUMBER', 'RRP_DATE', 'RRP_BY', 'NAME_RRPBY', 'ETA', 'STANDARD STATUS', 'GENERAL STATUS', 'STATUS_y']]

        dataMergeAllFiltered.reset_index(drop=True,inplace=True)

        # Menyeragamkan tanggal menjadi Y-m-d
        def convert_date_format(date_str):
            if pd.isna(date_str):
                return date_str
            try:
                date_obj = pd.to_datetime(date_str, format='%d-%b-%y')
                return date_obj.strftime('%d-%m-%y')
            except ValueError:
                return date_str

        def convert_date_format2(date_str):
            if pd.isna(date_str):
                return date_str
            try:
                date_obj = pd.to_datetime(date_str, format='%d-%m-%y')
                return date_obj.strftime('%Y-%m-%d')
            except ValueError:
                return date_str

        dataMergeAllFiltered['DATE AWB OUT_x'] = dataMergeAllFiltered['DATE AWB OUT_x'].apply(convert_date_format)
        dataMergeAllFiltered['DATE AWB OUT_x'] = dataMergeAllFiltered['DATE AWB OUT_x'].apply(convert_date_format2)

        dataMergeAllFiltered['AUTHORIZATION_DATE'] = dataMergeAllFiltered['AUTHORIZATION_DATE'].apply(convert_date_format)
        dataMergeAllFiltered['AUTHORIZATION_DATE'] = dataMergeAllFiltered['AUTHORIZATION_DATE'].apply(convert_date_format2)

        dataMergeAllFiltered['AUTHRQ_DATE'] = dataMergeAllFiltered['AUTHRQ_DATE'].apply(convert_date_format)
        dataMergeAllFiltered['AUTHRQ_DATE'] = dataMergeAllFiltered['AUTHRQ_DATE'].apply(convert_date_format2)

        dataMergeAllFiltered['RRP_DATE'] = dataMergeAllFiltered['RRP_DATE'].apply(convert_date_format)
        dataMergeAllFiltered['RRP_DATE'] = dataMergeAllFiltered['RRP_DATE'].apply(convert_date_format2)

        
        #dataMergeAllFiltered['CREATED DATE_x'] = pd.to_datetime(dataMergeAllFiltered['CREATED DATE_x'], errors='coerce', format='%d/%m/%Y')
        dataMergeAllFiltered['CREATED DATE_x'] = dataMergeAllFiltered['CREATED DATE_x'].apply(convert_date_format2)
        # prompt: Create a function to check the feature 'RRP DATE' in file 'dataMerge.xlsx' in sheet 'Sheet 1' according to the quartile of the day. I would like 3 quartiles of the day, day 1-10 (named Q1), 11-20 (named Q2), and 21-30 (named Q3). Then apply the function to the dataframe in a new column 'Quartile' with the formula YYYY-MM-Quartile, so for example 5 March 2025 the data in the column 'Quartile' would be 2025-03-Q1

        # Assigning quartile to created Date
        def assign_quartile_created(date_str):
            try:
                date_obj = pd.to_datetime(date_str)
                day = date_obj.day
                year = date_obj.year
                month = date_obj.month
                if 1 <= day <= 10:
                    return f"{year}-{month:02}-Q1"
                elif 11 <= day <= 20:
                    return f"{year}-{month:02}-Q2"
                elif 21 <= day <= 31:
                    return f"{year}-{month:02}-Q3"
                else:
                    return "Invalid Date"
            except (ValueError, TypeError):
                return "Invalid Date"

        def assign_quartile_rrp(date_str):
            try:
                date_obj = pd.to_datetime(date_str)
                day = date_obj.day
                year = date_obj.year
                month = date_obj.month
                if 1 <= day <= 10:
                    return f"{year}-{month:02}-Q1"
                elif 11 <= day <= 20:
                    return f"{year}-{month:02}-Q2"
                elif 21 <= day <= 31:
                    return f"{year}-{month:02}-Q3"
                else:
                    return "Invalid Date"
            except (ValueError, TypeError):
                return "Invalid Date"

        dataMergeAllFiltered['Quartile_RRP'] = dataMergeAllFiltered['RRP_DATE'].apply(assign_quartile_rrp)
        dataMergeAllFiltered['Quartile_Shipped'] = dataMergeAllFiltered['DATE AWB OUT_x'].apply(assign_quartile_rrp)
        dataMergeAllFiltered['Quartile_Created'] = dataMergeAllFiltered['CREATED DATE_x'].apply(assign_quartile_created)
        dataMergeAllFiltered['Date_Shipped'] = dataMergeAllFiltered['DATE AWB OUT_x'].fillna('Invalid Date')

        dataMergeAllFiltered.drop_duplicates(subset=['ORDER_TYPE-NUMBER-LINE'], inplace=True, keep='last')

        oldNewDate = pd.to_datetime(dataMergeAllFiltered['CREATED DATE_x'], errors='coerce')
        oldestDate = oldNewDate.min()
        oldestDate = oldestDate.strftime('%Y-%m-%d')

        newestDate = oldNewDate.max()
        newestDate = newestDate.strftime('%Y-%m-%d')

        dataMergeAllFiltered['ORDER NUMBER_x'] = pd.to_numeric(dataMergeAllFiltered['ORDER NUMBER_x'], errors='coerce')
        dataMergeAllFiltered.dropna(subset=['ORDER NUMBER_x'], inplace=True)
        dataMergeAllFiltered.reset_index(inplace=True)
        dataMergeAllFiltered.drop(columns=['index'], inplace=True)

        return dataMergeAllFiltered, oldestDate, newestDate

    except Exception as e:
        raise ValueError(f"Terjadi kesalahan saat memproses merge data: {e}")

# Fungsi untuk pivot data
def process_pivot_data(dataMergeAllFiltered):
    try:
        ## --- Beginning of pivotCreated_RRP --- 
        pivotCreated_RRP = dataMergeAllFiltered.pivot_table(index='Quartile_Created', columns='Quartile_RRP', values='ORDER_TYPE-NUMBER-LINE', aggfunc='count')

        cancelCountCreated_RRP = dataMergeAllFiltered[
            (dataMergeAllFiltered['Quartile_RRP'].notna()) &
            (dataMergeAllFiltered['Quartile_RRP'] != '') &
            (dataMergeAllFiltered['STATUS_x'] == 'CANCEL')
        ].groupby('Quartile_Created')['ORDER_TYPE-NUMBER-LINE'].count()
        pivotCreated_RRP.insert(0, 'Cancelled', value = cancelCountCreated_RRP)

        totalCountCreated_RRP = dataMergeAllFiltered[
            (dataMergeAllFiltered['Quartile_RRP'].notna()) &
            (dataMergeAllFiltered['Quartile_RRP'] != '') &
            (dataMergeAllFiltered['STATUS_x'] != 'CANCEL')
        ].groupby('Quartile_Created')['ORDER_TYPE-NUMBER-LINE'].count()
        pivotCreated_RRP.insert(0, 'Beginning Balance', value = totalCountCreated_RRP)

        nfDateCreated_RRP = dataMergeAllFiltered[
            (dataMergeAllFiltered['Quartile_RRP'].isna() | (dataMergeAllFiltered['Quartile_RRP'] == 'Invalid Date')) &
            (dataMergeAllFiltered['STATUS_x'] != 'CANCEL')
        ].groupby('Quartile_Created')['ORDER_TYPE-NUMBER-LINE'].count()
        pivotCreated_RRP.insert(2, 'Part Not Yet Received', value = nfDateCreated_RRP)

        pivotCreated_RRP.drop(columns='Invalid Date', inplace=True)

        # Add MultiIndex as Header of Header
        pivotCreated_RRP = pivotCreated_RRP.rename_axis(None, axis=1)
        new_columns = []

        for col in pivotCreated_RRP.columns:
            if col != 'Beginning Balance' and col != 'Cancelled' and col != 'Part Not Yet Received':
                new_columns.append(('Received Date', col))
            else:
              new_columns.append(('Status', col))

        pivotCreated_RRP.columns = pd.MultiIndex.from_tuples(new_columns)
        ## --- End of pivotCreated_RRP --- 

        ## --- Beginning of pivotCreated_Shipment --- 

        pivotCreated_Shipment = dataMergeAllFiltered.pivot_table(index='Quartile_Created', columns='Date_Shipped', values='ORDER_TYPE-NUMBER-LINE', aggfunc='count')
        pivotCreated_Shipment = pivotCreated_Shipment.sort_index(axis=1)

        cancelCountCreated_Shipment = dataMergeAllFiltered[
            (dataMergeAllFiltered['Date_Shipped'].notna()) &
            (dataMergeAllFiltered['Date_Shipped'] != '') &
            (dataMergeAllFiltered['STATUS_x'] == 'CANCEL')
        ].groupby('Quartile_Created')['ORDER_TYPE-NUMBER-LINE'].count()
        pivotCreated_Shipment.insert(0, 'Cancelled', value = cancelCountCreated_Shipment)

        totalCountCreated_Shipment = dataMergeAllFiltered[
            (dataMergeAllFiltered['Date_Shipped'].notna()) &
            (dataMergeAllFiltered['Date_Shipped'] != '') &
            (dataMergeAllFiltered['STATUS_x'] != 'CANCEL')
        ].groupby('Quartile_Created')['ORDER_TYPE-NUMBER-LINE'].count()
        pivotCreated_Shipment.insert(0, 'Beginning Balance', value = totalCountCreated_Shipment)

        nfDateCreated_Shipment = dataMergeAllFiltered[
            (dataMergeAllFiltered['Date_Shipped'].isna() | (dataMergeAllFiltered['Date_Shipped'] == 'Invalid Date')) &
            (dataMergeAllFiltered['STATUS_x'] != 'CANCEL')
        ].groupby('Quartile_Created')['ORDER_TYPE-NUMBER-LINE'].count()
        pivotCreated_Shipment.insert(2, 'Part Not Yet Received', value = nfDateCreated_Shipment)

        pivotCreated_Shipment.drop(columns='Invalid Date', inplace=True)


        pivotCreated_Shipment = pivotCreated_Shipment.rename_axis(None, axis=1)
        new_columns = []

        for col in pivotCreated_Shipment.columns:
            if col != 'Beginning Balance' and col != 'Cancelled' and col != 'Part Not Yet Received':
                new_columns.append(('Received Date', col))
            else:
              new_columns.append(('Status', col))

        # pivotCreated_Shipment = pivotCreated_Shipment
        pivotCreated_Shipment.columns = pd.MultiIndex.from_tuples(new_columns)


        ## --- End of  pivotCreated_Shipment --- 

        ## --- Beginning of  pivotCreated_ShipmentQ --- 

        pivotCreated_ShipmentQ = dataMergeAllFiltered.pivot_table(index='Quartile_Created', columns='Quartile_Shipped', values='ORDER_TYPE-NUMBER-LINE', aggfunc='count')

        cancelCountCreated_ShipmentQ = dataMergeAllFiltered[
            (dataMergeAllFiltered['Quartile_Shipped'].notna()) &
            (dataMergeAllFiltered['Quartile_Shipped'] != '') &
            (dataMergeAllFiltered['STATUS_x'] == 'CANCEL')
        ].groupby('Quartile_Created')['ORDER_TYPE-NUMBER-LINE'].count()
        pivotCreated_ShipmentQ.insert(0, 'Cancelled', value = cancelCountCreated_ShipmentQ)

        totalCountCreated_ShipmentQ = dataMergeAllFiltered[
            (dataMergeAllFiltered['Quartile_Shipped'].notna()) &
            (dataMergeAllFiltered['Quartile_Shipped'] != '') &
            (dataMergeAllFiltered['STATUS_x'] != 'CANCEL')
        ].groupby('Quartile_Created')['ORDER_TYPE-NUMBER-LINE'].count()
        pivotCreated_ShipmentQ.insert(0, 'Beginning Balance', value = totalCountCreated_ShipmentQ)

        nfDateCreated_ShipmentQ = dataMergeAllFiltered[
            (dataMergeAllFiltered['Quartile_Shipped'].isna() | (dataMergeAllFiltered['Quartile_Shipped'] == 'Invalid Date')) &
            (dataMergeAllFiltered['STATUS_x'] != 'CANCEL')
        ].groupby('Quartile_Created')['ORDER_TYPE-NUMBER-LINE'].count()
        pivotCreated_ShipmentQ.insert(2, 'Part Not Yet Received', value = nfDateCreated_ShipmentQ)

        pivotCreated_ShipmentQ.drop(columns='Invalid Date', inplace=True)

        # Add MultiIndex as Header of Header
        pivotCreated_ShipmentQ = pivotCreated_ShipmentQ.rename_axis(None, axis=1)
        new_columns = []

        for col in pivotCreated_ShipmentQ.columns:
            if col != 'Beginning Balance' and col != 'Cancelled' and col != 'Part Not Yet Received':
                new_columns.append(('Received Date', col))
            else:
              new_columns.append(('Status', col))

        pivotCreated_ShipmentQ.columns = pd.MultiIndex.from_tuples(new_columns)

        ## --- End of  pivotCreated_ShipmentQ --- 

        oldestDate = dataMergeAllFiltered['CREATED DATE_x'].min()
        newestDate = dataMergeAllFiltered['CREATED DATE_x'].max()

        return pivotCreated_RRP, pivotCreated_Shipment, pivotCreated_ShipmentQ, oldestDate, newestDate

    except Exception as e:
        raise ValueError(f"Terjadi kesalahan saat memproses pivot data: {e}")

