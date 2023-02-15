#from cmath import nan
from ast import Delete
from genericpath import exists
from operator import truediv
from pickle import TRUE
from re import T
import subprocess
import time
from unittest import result
import uuid
from sqlalchemy import tablesample, true
# from cv2 import dft
import tabula
from tabula import read_pdf  
import pandas as pd
import numpy as np
import os
import PyPDF2
import glob 
import multiprocessing
import multiprocessing.forkserver
import shutil
from multiprocessing import Pool  
from pathlib import Path

# os.system('cls' if os.name == 'nt' else 'clear')

# Declare PDF

def process_doc(doc): 
    
    pdf = doc

    print('Processing: ' + pdf)

    pdfObj = open(pdf, 'rb')

    reader = PyPDF2.PdfFileReader(pdfObj, strict=False)
    lastPageMax = reader.getNumPages() 

    text_file = open("./Ahold_Slotting/Output.txt", "a")
    text_file.write('Processing: %s\n' % pdf)

    pd.set_option("display.expand_frame_repr", False)

    results = pd.DataFrame()

#Scrape Second page
    text_file.write('Getting Table Info\n')
    secondPgGraph = tabula.read_pdf(pdf, pages='2', area=[80, 50, 800, 750], pandas_options={'header': None}, guess=True)

    for j in range(0, len(secondPgGraph[0])):
        if '.com' in str(secondPgGraph[0][0][j]):
            comLoc = str(secondPgGraph[0][0][j]).find('.com')
            secondPgGraph[0][7][j] = str(secondPgGraph[0][0][j])[:comLoc+4]
            secondPgGraph[0][0][j] = str(secondPgGraph[0][0][j])[comLoc+4:]

    for set in secondPgGraph:
        results = results.append(set, ignore_index=True)

    emptyList = []
    for i in range(0, len(results)):
        emptyList.append('6')
    # split column into multiple columns by delimiter

    if 'UPC AHOLD#' in str(results[0][0]):
        results.insert(0, 'UPC', emptyList, True)
        results[['UPC', 0]] = results[0].str.split(' ', expand=True)
    if 'UNFI# BRAND' in str(results[1][0]):
        results.insert(2, 'UNFI#', emptyList, True)
        results[['UNFI#', 1]] = results[1].str.split(' ', 1, expand=True)
    if 'PACK SIZE' in str(results[3][0]):
        results.insert(5, 'PACK', emptyList, True)    
        results[['PACK', 3]] = results[3].str.split(' ', 1, expand=True)
    if 'AHOLD# UNFI' in str(results[1][0]):
        for i in range(1,len(results)):
            brandString = str(results[1][i])[-3:]
            if brandString.isnumeric() == False:
                x = str(results[1][i]).find(' ', 8, 15)
                results.at[i, 2] = str(results[1][i])[x:]
                results.at[i, 1] = str(results[1][i])[:x]
        results.insert(2, 'AHOLD#', emptyList, True)
        results[['AHOLD#', 1]] = results[1].str.split(' ', expand=True)
    if 'PACK SIZE' in str(results[4][0]):
        results.insert(4, 'PACK', emptyList, True)
        results[['PACK', 4]] = results[4].str.split(' ', 1, expand=True)
    if 'PV REMIT' in str(results[12][0]):
        results.insert(12, 'PV', emptyList, True)
        results[['PV', 12]] = results[12].str.split(' ', 1, expand=True)
    
    results = results.drop(results.index[len(results)-1])
    text_file.write('Removing Nan\n')
    for i in range(0,12):
        string2 = str(results[i][2])
        if string2 == 'nan':
            results = results.drop(i, axis=1)

    #name colunms
    for i in range(0,14):
        try:
            results.rename(columns= {i:str(results[i][0])}, inplace=True)
        except:
            continue

    results = results.drop(results.index[0])

    for i in range(1,len(results)):
        strng = str(results.at[i,'BRAND'])
        if strng.isnumeric() == True:
            temp = results.at[i, 'BRAND']
            results.at[i, 'BRAND'] = results.at[i, 'UNFI#']
            results.at[i, 'UNFI#'] = temp
    text_file.write('Table Recorded\n')

#Scrape First Page
    text_file.write('Getting Header Info\n')
    header = tabula.read_pdf(pdf, stream=True, pages='1', area=[100, 0, 180, 815], pandas_options={'header': None}, guess=TRUE)
    #gets header into one string
    temp = ''.join(str(e) for e in header).replace('NaN', '').replace('  ', ' ')
    headerList = temp.split()

    remitLoc = 0
    billLoc = 0
    deductNumLoc = 0
    invoiceLoc = 0

    for i in range(0, len(headerList)):
        if headerList[i] == 'Bill':
            billLoc = i
        elif headerList[i] == 'Deduction':
            deductNumLoc = i
        elif headerList[i] == 'Invoice':
            invoiceLoc = i

    bill = ''
    deductNum = ''

    deductNum = headerList[deductNumLoc+2]

    for j in range(billLoc+1, invoiceLoc):
        bill = bill + headerList[j] + ' ' 

    first_page = tabula.read_pdf(pdf, pages='1',area=[220, 0, 250, 815], pandas_options={'header': None}, guess=True) 

    #print(first_page)

    results['Bill To'] = bill
    results['Deduction Num'] = deductNum
    results['PV#'] = str(first_page[0][0][0])
    results['Billing Program'] = str(first_page[0][1][0])
    results['Billing Desc'] = str(first_page[0][2][0])
    results['DBS SJ#'] = str(first_page[0][3][0])
    results['VOUCHER INVOICE#'] = str(first_page[0][4][0])
    results['Admin Fee'] = str(first_page[0][6][0])

    results.insert(loc=0, column='File Name', value=(pdf.replace('.pdf', '')))
    text_file.write('Header Recorded\n\n')
    text_file.close()

    return(results)

def startConversion(docs):
    # On Windows calling this function is necessary.
    multiprocessing.freeze_support()     
    #subprocess.Popen(['java', '-jar', 'tabula-1.0.5-jar-with-dependencies.jar'])
    time.sleep(5)
    path = os.environ['PATH']

    if 'Java\jre' not in path:
        print('Path Does not Exist')
        os.environ['PATH'] = "{}{}{}".format('C:/Program Files (x86)/Java/jre1.8.0_281/bin', os.pathsep, path)
    else:
        print('Exist')

    # docs = glob.glob("*.pdf")
    
    debugging = False
    debug = ['00854998AHOLD - 1179.64 - slottingOCR.pdf']
    #debug = ['00852606AHOLD - 1820.54 - slotting OCR.pdf']

    text_file = open("./Ahold_Slotting/Output.txt", "w")
    print('Beginning conversion process.')
    text_file.write('Beginning conversion process. \n\n')
    text_file.close()

    UploadCode = str(uuid.uuid4()).upper() 

    with Pool(4) as p:        
        data = pd.DataFrame()
        if debugging:
            data = pd.concat(p.map(process_doc, debug)).reset_index(drop=True)
        else:
            data = pd.concat(p.map(process_doc, docs)).reset_index(drop=True)

        writer = pd.ExcelWriter('{}.xlsx'.format(UploadCode), engine='xlsxwriter')         
        
        data.to_excel(writer, sheet_name='Raw Converted', index=False)  

        for column in data:
            column_width = max(data[column].astype(str).map(len).max(), len(column))
            col_idx = data.columns.get_loc(column)
            writer.sheets['Raw Converted'].set_column(col_idx, col_idx, column_width)
        
        writer.save()
        
        docName = "{}.xlsx".format(UploadCode)
        text_file = open("./Ahold_Slotting/Output.txt", "a")
        text_file.write('Writing data to Excel \n')

    if(debugging == False):
        for doc in docs:
            shutil.move(doc, "./Ahold_Slotting/Processed/" + doc.split("\\")[1])
            print('Moving ' + doc + ' to processed folder.')
        text_file.write('PDFs Moved to Proccessed Folder \n')

     # opening EXCEL through Code
    #local path in dir
    absolutePath = Path('./{}'.format(docName)).resolve()
    os.system(f'start excel.exe "{absolutePath}"')
    text_file.write('Conversion Complete')
    text_file.close()

    print('Conversion completed, please close this window.')