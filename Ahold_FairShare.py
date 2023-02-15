#from cmath import nan
from operator import truediv
from pickle import TRUE
import subprocess
import time
import uuid
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
errorCheck = {'First Table': False, 'Tables': False, 'Last Table' : False, 'BillTo' : False, 
            'Deduction' : False, 'VendorNumber' : False, 'SubTotal' : False, 'SupplierTotal' : False, 'AdminFee' : False }

def process_doc(doc): 

    pdf = doc
    global errorMsg

    print('Processing: ' + pdf)
    text_file = open("./FairShare_export/Output.txt", "a")
    text_file.write('Processing: %s \n' % pdf)

    pdfObj = open(pdf, 'rb')

    reader = PyPDF2.PdfFileReader(pdfObj, strict=False)
    lastPageMax = reader.getNumPages()

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

# Get header data, ie. Bill to, Deduction num, and Vendor

    #header focuses on top of the page to get the Bill To, Deduction Num, and Renit Vendor data
    #header[list][column][row]
    text_file.write('Getting header \n')
    header = tabula.read_pdf(pdf, stream=True, pages='1', area=[100, 0, 180, 815], pandas_options={'header': None}, guess=TRUE)

    #gets header into one string
    temp = ''.join(str(e) for e in header).replace('NaN', '').replace('  ', ' ')

    # Find Index of Bill TO and index of Index of Deduction Number and get everything between 
    # Find index of Num in Deduction Num then add + 1 to find the deduction numbers
    # Find index of Remit Vendor then add + 1 to find remit vendor number 

    #splits string into long list
    splitOnSpace = temp.split()

    billToLoc = 0
    deductionLoc = 0
    vendorLoc = 0

    for i in range(0, len(splitOnSpace)):
        if splitOnSpace[i] == 'TO':
            billToLoc = i
        elif splitOnSpace[i] == 'Deduction':
            deductionLoc = i
        elif splitOnSpace[i] == 'Vendor':
            vendorLoc = i
    
    billTo = ''
    for i in range(billToLoc+1 ,deductionLoc):
        billTo = billTo + splitOnSpace[i] + ' '
    deduction = splitOnSpace[deductionLoc + 2]
    vendor = splitOnSpace[vendorLoc + 1]

    text_file.write('Got Bill To, Deduction, and Vendor \n')
    results = pd.DataFrame()
    #if pdf is over 2 pages long
    text_file.write('Getting Tables \n')

    if(lastPageMax > 2):

        tableOne = tabula.read_pdf(pdf, pages=1 , area=[230, 0, 1056, 815], pandas_options={'header': None}, guess=TRUE)

        tables = tabula.read_pdf(pdf, pages=list(range(2,lastPageMax)), pandas_options={'header': None}, guess=TRUE)

        lastTable = tabula.read_pdf(pdf, pages=lastPageMax, pandas_options={'header': None}, guess=TRUE)

        for n in range(0,len(tableOne[0][0])):
            if(str(tableOne[0][0][n]) == 'nan'):
                for m in range(0,11):
                    if(str(tableOne[0][m][n]) != 'nan'):
                        tableOne[0][m][n-1] = tableOne[0][m][n-1] + tableOne[0][m][n]
        for l in range(0,len(tables)):
            for n in range(0,len(tables[l][0])):
                if(str(tables[l][0][n]) == 'nan'):
                    for m in range(0,11):
                        if(str(tables[l][m][n]) != 'nan'):
                            tables[l][m][n-1] = tables[l][m][n-1] + tables[l][m][n]
                if(len(tables[l][6][len(tables[l][0])-1]) < len(tables[l][6][len(tables[l][0])-3])):
                    tables[l][6][len(tables[l][0])-1] = tables[l][6][len(tableOne[l][0])-2]
        if (lastTable != []):
            for n in range(0,len(lastTable[0][0])):
                if(str(lastTable[0][0][n]) == 'nan'):
                    for m in range(0,11):
                        if(str(lastTable[0][m][n]) != 'nan'):
                            lastTable[0][m][n-1] = lastTable[0][m][n-1] + ' ' + lastTable[0][m][n]

        #set column titles
        for set in tableOne:
            set.columns = ['DC', 'Customer Name', 'Item Num', 'Billing Description', 'UPC', 'Pack/Brand', 'Preform Dates', 'Percentage', 'QTY', 'Amount', 'Total']
            results = results.append(set, ignore_index=True)
        for set in tables:
            set.columns = ['DC', 'Customer Name', 'Item Num', 'Billing Description', 'UPC', 'Pack/Brand', 'Preform Dates', 'Percentage', 'QTY', 'Amount', 'Total']
            results = results.append(set, ignore_index=True)
        for set in lastTable:
            set.columns = ['DC', 'Customer Name', 'Item Num', 'Billing Description', 'UPC', 'Pack/Brand', 'Preform Dates', 'Percentage', 'QTY', 'Amount', 'Total']
            results = results.append(set, ignore_index=True)
       
    #if pdf is only 2 pages long
    elif(lastPageMax == 2):
 
        tableOne = tabula.read_pdf(pdf, pages=1 , area=[230, 0, 1056, 815], pandas_options={'header': None}, guess=TRUE)

        lastTable = tabula.read_pdf(pdf, pages=lastPageMax, pandas_options={'header': None}, guess=TRUE)

        if lastTable == []:
            lastTable = tabula.read_pdf(pdf, pages=lastPageMax, area=[80, 0, 130, 815], pandas_options={'header': None}, guess=TRUE)

        for n in range(0,len(tableOne[0][0])):
            if(str(tableOne[0][0][n]) == 'nan'):
                for m in range(0,11):
                    if(str(tableOne[0][m][n]) != 'nan'):
                        tableOne[0][m][n-1] = tableOne[0][m][n-1] + tableOne[0][m][n]
        if (lastTable != []):
            for n in range(0,len(lastTable[0][0])):
                if(str(lastTable[0][0][n]) == 'nan'):
                    for m in range(0,11):
                        if(str(lastTable[0][m][n]) != 'nan'):
                            lastTable[0][m][n-1] = lastTable[0][m][n-1] + lastTable[0][m][n]

        #set column titles
        for set in tableOne:
            set.columns = ['DC', 'Customer Name', 'Item Num', 'Billing Description', 'UPC', 'Pack/Brand', 'Preform Dates', 'Percentage', 'QTY', 'Amount', 'Total']
            results = results.append(set, ignore_index=True)
        for set in lastTable:
            set.columns = ['DC', 'Customer Name', 'Item Num', 'Billing Description', 'UPC', 'Pack/Brand', 'Preform Dates', 'Percentage', 'QTY', 'Amount', 'Total']
            results = results.append(set, ignore_index=True)


    text_file.write('Tables Recorded \n')
# Get Totals at end of table
    text_file.write('Getting Totals and AdminFee \n')
    tableTotals = tabula.read_pdf(pdf, pages=(lastPageMax-1), pandas_options={'header': None}, guess=False)

    temp1 = ''.join(str(e) for e in tableTotals).replace('NaN', '').replace('  ', ' ')

    #splits string into long list
    splitLastPage = temp1.split()

    SubtotalLoc = 0
    AdminfeeLoc = 0
    SupplierTotalLoc = 0

    Subtotal = ''
    Adminfee = ''
    SupplierTotal = ''

    for j in range(0,len(splitLastPage)):
        if splitLastPage[j] == 'Subtotal':
            SubtotalLoc = j
        elif splitLastPage[j] == 'AdminFee':
            AdminfeeLoc = j
        elif 'Supplier' in splitLastPage[j]:
            SupplierTotalLoc = j

    if SubtotalLoc == 0:
        tableTotals = tabula.read_pdf(pdf, pages=lastPageMax, pandas_options={'header': None}, guess=False)

        temp1 = ''.join(str(e) for e in tableTotals).replace('NaN', '').replace('  ', ' ')

        #splits string into long list
        splitLastPage = temp1.split()

        for j in range(0,len(splitLastPage)):
            if splitLastPage[j] == 'Subtotal':
                SubtotalLoc = j
            elif splitLastPage[j] == 'AdminFee':
                AdminfeeLoc = j
            elif 'Supplier' in splitLastPage[j]:
                SupplierTotalLoc = j

    Subtotal = splitLastPage[SubtotalLoc + 1]
    Adminfee = splitLastPage[AdminfeeLoc + 1]
    SupplierTotal = splitLastPage[SupplierTotalLoc + 1]
    text_file.write('Got Totals and AdminFee \n')

#Print to excel
    text_file.write('Recording extra data \n')
    adminFeeRow = ['','','', 'Admin Fee:','','','','','','', Adminfee]
    adminFeeDf = pd.DataFrame([adminFeeRow])
    adminFeeDf.columns = ['DC', 'Customer Name', 'Item Num', 'Billing Description', 'UPC', 'Pack/Brand', 'Preform Dates', 'Percentage', 'QTY', 'Amount', 'Total']
    results = results.append(adminFeeDf)
    results['BillTo'] = billTo
    results['Deduction'] = deduction
    results['VendorNumber'] = vendor
    results['SubTotal'] = Subtotal
    results['SupplierTotal'] = SupplierTotal

    results.insert(loc=0, column='File Name', value=(pdf.replace('.pdf', '')))

    text_file.write('Extra data Recorded\n\n')
    text_file.close()

    resultLen = len(results.index)
    listNan = []

    for i in range(0,resultLen):
        if(str(results.iloc[i, 1]) == 'nan'):
            listNan.append(i)
    for j in listNan:
        results.drop(j, inplace=True)

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

    text_file = open("./FairShare_export/Output.txt", "w")
    
    #docs = glob.glob("*.pdf")
    
    debugging = False
    debug = ['FSRAHOLD1221010387B OCR.pdf']
    #debug = ['FSRAHOLD0722A010371  1793.08 OCR.pdf']
    #debug = ['FSRAHOLD1221010371  5711.85 OCR.pdf']
    #debug = ['FSRAHOLD0722A010387 OCR.pdf']

    print('Beginning conversion process.')
    text_file.write('Beginning conversion process. \n\n')
    text_file.close()

    # #Used for Debugging
    # if debugging:
    #     for doc in debug:
    #         process_doc(doc)
    # else:
    #     for doc in docs:
    #         process_doc(doc) 
    
    #Used for exporting to Excel file
    UploadCode = str(uuid.uuid4()).upper() 

    with Pool(4) as p:    
        data = pd.DataFrame()
        if debugging:
            data = pd.concat(p.map(process_doc, debug)).reset_index(drop=True)
        else:
            data = pd.concat(p.map(process_doc, docs)).reset_index(drop=True)
        # data = pd.concat(process_doc(doc)).reset_index(drop=True)

        writer = pd.ExcelWriter('{}.xlsx'.format(UploadCode), engine='xlsxwriter')         
        
        data.to_excel(writer, sheet_name='Raw Converted', index=False)  

        for column in data:
            column_width = max(data[column].astype(str).map(len).max(), len(column))
            col_idx = data.columns.get_loc(column)
            writer.sheets['Raw Converted'].set_column(col_idx, col_idx, column_width)
        
        writer.save()
        
        docName = "{}.xlsx".format(UploadCode)
        text_file = open("./FairShare_export/Output.txt", "a")
        text_file.write('Writing data to Excel \n')
    
    if(debugging == False):
        for doc in docs:
            shutil.move(doc, "./FairShare_export/Processed/" + doc.split("\\")[1])
            print('Moving ' + doc + ' to processed folder.')
        text_file.write('PDFs Moved to Proccessed Folder \n')
    # opening EXCEL through Code
    #local path in dir
    absolutePath = Path('./{}'.format(docName)).resolve()
    os.system(f'start excel.exe "{absolutePath}"')
    
    text_file.write('Conversion Complete')
    text_file.close()

    print('Conversion completed, please close this window.')