#Azure SQL Liberies 
import sqlalchemy
import sqlalchemy.sql.default_comparator
from sqlalchemy import create_engine
import pyodbc
from urllib.parse import quote_plus, scheme_chars
from openpyxl import load_workbook
import threading
import uuid
import math
#Conversion Liberies 
import pandas as pd
import numpy as np
import os
import glob
from datetime import datetime
from functools import partial
import multiprocessing
import multiprocessing.forkserver
from multiprocessing import Pool  
import subprocess
import os 
import sys
import shutil
from pathlib import Path

class _Popen(subprocess.Popen):
    def __init__(self, *args, **kw):
        if hasattr(sys, 'frozen'):
            # We have to set original _MEIPASS2 value from sys._MEIPASS
            # to get --onefile mode working.
            os.putenv('_MEIPASS2', sys._MEIPASS)
        try:
            super(_Popen, self).__init__(*args, **kw)
        finally:
            if hasattr(sys, 'frozen'):
                # On some platforms (e.g. AIX) 'os.unsetenv()' is not
                # available. In those cases we cannot delete the variable
                # but only set it to the empty string. The bootloader
                # can handle this case.
                if hasattr(os, 'unsetenv'):
                    os.unsetenv('_MEIPASS2')
                else:
                    os.putenv('_MEIPASS2', '')

class Process(multiprocessing.Process):
    _Popen = _Popen

def process_xlsx(doc):
    results = pd.DataFrame()   
    results = pd.read_excel(doc, sheet_name=0, index_col=None, converters={'Auth #':str}, engine='openpyxl')
    results['Doc'] = doc
   
    print('Successfully Converted ' + doc)
    return results 


def process_csv(doc):
    results = pd.DataFrame()       
    results = pd.read_csv(doc, index_col=None, converters={'Auth #':str})
    results['Doc'] = doc
   
    print('Successfully Converted ' + doc)
    return results 

data_xlsx = None
data_csv = None

def startConversion(csv_docs, xlsx_docs):   
    # On Windows calling this function is necessary.
    multiprocessing.freeze_support()     
    master_doc = glob.glob("UNFIEast_Master_*.xlsx")
    if master_doc:
        print("Removing existing master.xlsx doc")
        os.remove(str(master_doc[0]))
    # csv_docs = glob.glob("*.csv")
    # xlsx_docs = glob.glob("*.xlsx")
    # print(csv_docs)
    # print(xlsx_docs)
      
    UploadCode = str(uuid.uuid4()).upper()
    print('Beginning conversion and upload process.')

    text_file = open("./UNFIEast_Master/Output.txt", "w")
    text_file.write('Beginning conversion process. \n\n')
    text_file.close()

    with Pool(4) as p:        
        final_data = pd.DataFrame()
        data_xlsx = pd.DataFrame()
        data_csv = pd.DataFrame()
        text_file = open("./UNFIEast_Master/Output.txt", "a")
        if len(xlsx_docs) != 0:
            text_file.write('Converting xlsx files \n')
            data_xlsx = pd.concat(p.map(process_xlsx, xlsx_docs)).reset_index(drop=True)
        if len(csv_docs) != 0:
            text_file.write('Converting csv files \n')
            data_csv = pd.concat(p.map(process_csv, csv_docs)).reset_index(drop=True)
        if data_xlsx.empty == False and data_csv.empty == False: 
            text_file.write('Joining xlsx and csv data \n')           
            final_data = pd.concat([data_xlsx, data_csv])
            final_data['Customer Name'].replace('', np.nan, inplace=True)
            final_data.dropna(subset=['Customer Name'], inplace=True)
            final_data['ChgBckAmt'] = final_data['ChgBckAmt'].apply(str)
            final_data['ChgBckAmt'] = final_data['ChgBckAmt'].replace('\$|,', '', regex=True)
            final_data['ChgBckAmt'] = final_data['ChgBckAmt'].astype(float)
            final_data['InvAmt'] = final_data['InvAmt'].apply(str)
            final_data['InvAmt'] = final_data['InvAmt'].replace('\$|,', '', regex=True)
            final_data['InvAmt'] = final_data['InvAmt'].astype(float)        
            final_data['ChgBckPct'] = final_data['ChgBckPct'].astype(int)
            text_file.write('Xlsx and csv data labeled\n') 
        elif data_xlsx.empty == False and data_csv.empty == True:
            final_data = data_xlsx
            text_file.write('Labeling csv data\n') 
            final_data['Customer Name'].replace('', np.nan, inplace=True)
            final_data.dropna(subset=['Customer Name'], inplace=True)
            final_data['ChgBckAmt'] = final_data['ChgBckAmt'].apply(str)
            final_data['ChgBckAmt'] = final_data['ChgBckAmt'].replace('\$|,', '', regex=True)
            final_data['ChgBckAmt'] = final_data['ChgBckAmt'].astype(float)
            final_data['InvAmt'] = final_data['InvAmt'].apply(str)
            final_data['InvAmt'] = final_data['InvAmt'].replace('\$|,', '', regex=True)
            final_data['InvAmt'] = final_data['InvAmt'].astype(float)        
            final_data['ChgBckPct'] = final_data['ChgBckPct'].astype(int)
            text_file.write('Xlsx data labeled\n') 
        elif data_xlsx.empty == True and data_csv.empty == False:
            final_data = data_csv
            text_file.write('Labeling xlsx data\n') 
            final_data['Customer Name'].replace('', np.nan, inplace=True)
            final_data.dropna(subset=['Customer Name'], inplace=True)
            final_data['ChgBckAmt'] = final_data['ChgBckAmt'].apply(str)
            final_data['ChgBckAmt'] = final_data['ChgBckAmt'].replace('\$|,', '', regex=True)
            final_data['ChgBckAmt'] = final_data['ChgBckAmt'].astype(float)
            final_data['InvAmt'] = final_data['InvAmt'].apply(str)
            final_data['InvAmt'] = final_data['InvAmt'].replace('\$|,', '', regex=True)
            final_data['InvAmt'] = final_data['InvAmt'].astype(float)        
            final_data['ChgBckPct'] = final_data['ChgBckPct'].astype(int)
            text_file.write('Xlsx data labeled\n') 
        print('Conversion completed, beginning upload process.')
        
        writer = pd.ExcelWriter('UNFIEast_Master_{}.xlsx'.format(UploadCode), engine='xlsxwriter')
   
        final_data.to_excel(writer, sheet_name='Raw Converted', index=False)  
        
        for column in final_data:
            column_width = max(final_data[column].astype(str).map(len).max(), len(column))
            col_idx = final_data.columns.get_loc(column)
            writer.sheets['Raw Converted'].set_column(col_idx, col_idx, column_width)
    
        writer.save()
          
        docName = "UNFIEast_Master_{}.xlsx".format(UploadCode)
        text_file.write('Writing data to Excel \n')

    for doc in csv_docs:
        shutil.move(doc, "./UNFIEast_Master/Processed/" + doc.split("\\")[1])
        print('Moving ' + doc + ' to processed folder.')
    text_file.write('PDFs Moved to Proccessed Folder \n')

    for doc in xlsx_docs:
        shutil.move(doc, "./UNFIEast_Master/Processed/" + doc.split("\\")[1])
        print('Moving ' + doc + ' to processed folder.')
    text_file.write('PDFs Moved to Proccessed Folder \n')
                    
    # opening EXCEL through Code
    #local path in dir
    absolutePath = Path('./{}'.format(docName)).resolve()
    os.system(f'start excel.exe "{absolutePath}"')
    text_file.write('Conversion Complete')
    text_file.close()

    print('Conversion completed, please close this window.')
