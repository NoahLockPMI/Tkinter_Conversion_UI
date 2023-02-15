import math
import time
import uuid
from matplotlib.pyplot import axis
import tabula
from tabula import read_pdf  
import pandas as pd
import numpy as np
import re
import os
import PyPDF2
import glob 
import multiprocessing
import multiprocessing.forkserver
import shutil
from multiprocessing import Pool  
import subprocess
from pathlib import Path

# os.system('cls' if os.name == 'nt' else 'clear')

# Declare PDF

def process_doc(doc): 
    
    pdf = doc

    pdfObj = open(pdf, 'rb')

    reader = PyPDF2.PdfFileReader(pdfObj, strict=False)

    print('Processing: ' + pdf)
    text_file = open("./Ahold_Scan/Output.txt", "a")
    text_file.write('Processing: %s\n' % pdf)

    results = pd.DataFrame()

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    page = reader.getPage(0)
    text = str(page.extract_text())
    listOne = text.split()

    #Item infomation collected
    text_file.write('Getting Header Info')
    secondPgBottom = tabula.read_pdf(pdf, pages='2', area=[160, 0, 390, 815], pandas_options={'header': None}, guess=False)

    if str(secondPgBottom[0][2][0]) == 'nan':
        del secondPgBottom[0][2]
        secondPgBottom[0][2] = secondPgBottom[0].pop(3)
        secondPgBottom[0][3] = secondPgBottom[0].pop(4)
        secondPgBottom[0][4] = secondPgBottom[0].pop(5)

    data = {
        'Item #' : [],
        'Description' : [],
        'QTY' : [],
        'Unit Amount' : [],
        'Amount' : []
    }

    rowLength = len(secondPgBottom[0][0])

    for c in range(0,4):
        for d in range(0,rowLength):
            if 'PK: ' in str(secondPgBottom[0][c][d]):
                data['Item #'].append(str(secondPgBottom[0][c-1][d-1]))
                data['Description'].append(str(secondPgBottom[0][c][d-1]) + ' ' + str(secondPgBottom[0][c][d]))
                data['QTY'].append(str(secondPgBottom[0][c+1][d-1]))
                data['Unit Amount'].append(str(secondPgBottom[0][c+2][d-1]))
                data['Amount'].append(str(secondPgBottom[0][c+3][d-1]))
        
                
    dfBottom = pd.DataFrame(data)

    results = results.append(dfBottom)
    text_file.write('Header Info Recorded \n')

    #Set admin fee as own row
    text_file.write('Getting Admin Fee \n')
    first_pageAdmin = tabula.read_pdf(pdf, pages='1',area=[220, 0, 250, 815], pandas_options={'header': None}, guess=True)
    adminFee = str(first_pageAdmin[0][6][0])
    adminFeeRow = ['','Admin Fee:','','', adminFee]
    adminFeeDf = pd.DataFrame([adminFeeRow])
    adminFeeDf.columns = ['Item #', 'Description', 'QTY', 'Unit Amount', 'Amount']
    results = results.append(adminFeeDf) 
    text_file.write('Admin Fee Recorded\n')

    text_file.write('Getting Remit, Bill, and Deduction \n')
    remitLoc = 0
    billLoc = 0
    deductNumLoc = 0

    for i in range(0, len(listOne)):
        if listOne[i] == 'Remit':
            remitLoc = i
        elif listOne[i] == 'Bill':
            billLoc = i
        elif listOne[i] == 'Deduction':
            deductNumLoc = i

    remit = ''
    bill = ''
    deductNum = ''

    remit = listOne[remitLoc+1]
    deductNum = listOne[deductNumLoc+4] #+4 or +6 need to check 
    deductNum = deductNum + listOne[deductNumLoc+5]

    for j in range(billLoc+1, deductNumLoc):
        bill = bill + listOne[j] + ' ' 

    pd.options.display.width=None

# Scraping first page
    text_file.write('Getting First Page Data \n')
    first_page = tabula.read_pdf(pdf, pages='1', area=[220, 0, 250, 815], pandas_options={'header': None}, guess=True) 

    results['PV#'] = str(first_page[0][0][0])
    results['Billing Program'] = str(first_page[0][1][0])
    results['Billing Desc'] = str(first_page[0][2][0])
    results['DBS SJ#'] = str(first_page[0][3][0])
    results['VOUCHER INVOICE#'] = str(first_page[0][4][0])
    results['Admin Fee'] = str(first_page[0][6][0])
    text_file.write('First Page Data Recorded\n')

# Scraping second page
    text_file.write('Getting Second Page Data\n')
    secondPgTop = tabula.read_pdf(pdf, pages='2', area=[50, 0, 150, 815], pandas_options={'header': None}, guess=False)

    invoice = ''
    dealDates = ''

    for a in range(0,4):
        for b in range(0,8):
            if 'INVOICE NO:' in str(secondPgTop[0][a][b]):
                invoice = str(secondPgTop[0][a][b])
                invoice = invoice[12:]
            elif 'DEAL DATES:' in str(secondPgTop[0][a][b]):
                dealDates = str(secondPgTop[0][a][b])
                dealDates = dealDates[12:]
                
    #Fill the empty lines with this information
    results['Remit'] = remit
    results['Deduction Num'] = deductNum
    results['Bill To'] = bill
    results['INVOICE NO:'] = invoice
    results['DEAL DATES:'] = dealDates

    results.insert(loc=0, column='File Name', value=(pdf.replace('.pdf', '')))
    text_file.write('Remit, Bill, and Deduction Recorded\n')
    text_file.write('Second Page Data Recorded\n\n')
    text_file.close()

    return(results)

def startConversion(docs):
    # On Windows calling this function is necessary.
    multiprocessing.freeze_support()     
    #subprocess.Popen(['java', '-jar', 'tika-server.jar'])
    time.sleep(5)
    
    #docs = glob.glob("Ahold_export\*.pdf")
    
    debugging = False
    #debug = ['00791572AHOLD.pdf']
    #debug = ['00791749AHOLD.pdf']
    debug = ['00858586AHOLD.pdf']
    #debug = ['00858747AHOLD  1560.90 OCR.pdf']
    #debug = ['00763427AHOLD 930.05 717959 OCR.pdf']
    
    text_file = open("./Ahold_Scan/Output.txt", "w")
    print('Beginning conversion process.')
    text_file.write('Beginning conversion process. \n\n')
    text_file.close()

    UploadCode = str(uuid.uuid4()).upper() 
    #Pool my ber messing with .exe
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
        text_file = open("./Ahold_Scan/Output.txt", "a")
        text_file.write('Writing data to Excel \n')

    if(debugging == False):
        for doc in docs:
            shutil.move(doc, "./Ahold_Scan/Processed/" + doc.split("\\")[1])
            print('Moving ' + doc + ' to processed folder.')
        text_file.write('PDFs Moved to Proccessed Folder \n')

    # opening EXCEL through Code
    #local path in dir
    absolutePath = Path('./{}'.format(docName)).resolve()
    os.system(f'start excel.exe "{absolutePath}"')
    text_file.write('Conversion Complete')
    text_file.close()

    print('Conversion completed, please close this window.')
    