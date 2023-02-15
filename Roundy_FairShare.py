from distutils.log import debug
import math
import time
from matplotlib import pyplot
from sqlalchemy import true
import tabula
import pandas as pd
import numpy as np
import re
import os
import PyPDF2
import glob 
import multiprocessing
import multiprocessing.forkserver
from multiprocessing import Pool  
import subprocess
from pathlib import Path
import uuid
import shutil
from datetime import date

#os.system('cls' if os.name == 'nt' else 'clear')

# Declare PDF
#def classify_doc(header):

def getAmount(word):
    toggle = False
    ret = ''
    for char in word:
        if not(toggle):
            if char=='$':
                toggle = True
        else:
            if char.isnumeric() or char=='.':
                ret += char
            else:
                return ret
    return ret

def getBodyIndex(column):
    counter = 0
    for row in column:
        if row == 'Item  Information':
            return counter
        counter += 1
    return counter

def getNextData(row):
    n = str.strip(row).find(' ',0)
    data = row[0:n]
    return data

def chopData(row):
    n = str.strip(row).find(' ',0) + 1
    data = row[n:]
    return data

def processData(data):
    #a = {ord(',') : 0, ord('$') : 0}
    try:
        data = data.replace(',','').replace('$','')
        return float(data)
    except:
        return data
    # try:
    #     if data[0] == '$':
    #         return float(data[1:].translate(a))
    #     else:
    #         return float(data)
    # except:
    #     return data

def process_doc(doc): 
    
    print('Converting this doc ' + doc)
    
    pdf = doc

    text_file = open("./Roundy_FairShare/Output.txt", "a")
    text_file.write('Processing: %s \n' % pdf)

    pdfObj = open(pdf, 'rb')

    reader = PyPDF2.PdfFileReader(pdfObj, strict=False)
    lastPageMax = reader.getNumPages()

    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    text_file.write('Getting Pages \n')
    pages = tabula.read_pdf(pdf, pages='all', pandas_options={'header': None}, guess=False)

    #for page in pages:
    #    print(page)
    header = pages[0]
    bodyStart = pages[1]
    footer = pages[lastPageMax-1].tail(6)
    
    #cut footer off last page
    pages[lastPageMax-1] = pages[lastPageMax - 1][:-5]
    #print(header)
    #print(bodyStart)
    #print(footer)
    text_file.write('Recording header \n')
    headerData = {
        "Invoice#" : '',
        "DC#" : '',
        "Chargeback / Invoice" : "Roundy's Merchandising Charges",
        "Time Frame" : '',
        "ChargeBack" : '',
        "EP FEE" : '',
        "Invoice Total" : '',
    }

    #get data from header
    for index, row in header.iterrows():
        wordList = row[0].split(r' ')
        if wordList[0] == 'Invoice':
            headerData['Invoice#'] = wordList[2]
            continue
        if wordList[0] == 'DC#:':
            headerData['DC#'] = wordList[1]
    text_file.write('Header Recorded\n')

    #get data from footer
    text_file.write('Recording footer \n')
    currencyStack = []
    for index, row in footer.iterrows():
        for field in row:
            amount = getAmount(str(field))
            if len(amount) > 0:
                currencyStack.append(amount)
    
    headerData['Invoice Total'] = processData(currencyStack.pop())
    headerData['EP FEE'] = processData(currencyStack.pop())
    headerData['ChargeBack'] = processData(currencyStack.pop())
    text_file.write('Footer Redcored\n')
    
    #get rest of header data
    text_file.write('Recording body \n')
    bodyIndexStart = getBodyIndex(bodyStart[0])
    secondHeader = bodyStart[0:bodyIndexStart]

    for row in secondHeader[1]:
        if str(row)[0:11] == 'Time Frame ':
            headerData['Time Frame'] = str(row)[11:]

    bodyPages = pages[1:]
    bodyPages[0] = bodyPages[0][bodyIndexStart+1:]
    
    body = pd.DataFrame()
    for page in bodyPages:
        page.reset_index(drop=True)
        body = pd.concat([body,page[:-1]])
    
    columnNames = ['UPC', 'Brand']
    
    for columnName in columnNames:
        body[columnName] = body[0].apply(lambda x: getNextData(x),1)
        body[0] = body[0].apply(lambda x: chopData(x))
    
    body.rename(columns = {0:'Product Description'}, inplace = True)

    columnNames = ['WholeSale','Qty']
    
    for columnName in columnNames:
        body[columnName] = body[1].apply(lambda x: getNextData(x),1)
        body[columnName] = body[columnName].apply(lambda x: processData(x),1)
        body[1] = body[1].apply(lambda x: chopData(x))

    body.rename(columns = {1:'Total'}, inplace = True)
    body['Total'] = body['Total'].apply(lambda x: processData(x),1)
    
    #adding in header data repeating
    for key in headerData:
        body[key] = headerData[key]
    
    # print(body)
    text_file.write('Body recorded \n')
    text_file.write('%s finished converting \n\n' % pdf)
    text_file.close()
    return body

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
    
    UploadCode = str(uuid.uuid4()).upper()

    debugging = False
    debug = ['RF42491118-RF42557618_166bc.pdf']

    
    text_file = open("./Roundy_FairShare/Output.txt", "w")
    text_file.write('Beginning conversion process. \n\n')
    text_file.close()
    
    print('Beginning conversion process.')
            
    data = pd.DataFrame()
    
    with Pool(4) as p:
        if debugging:
            data = pd.concat(p.map(process_doc, debug))  
        else:
            data = pd.concat(p.map(process_doc, docs))

        writer = pd.ExcelWriter('Roundys_Fairshare_{}.xlsx'.format(UploadCode), engine='xlsxwriter')
        
        text_file = open("./Roundy_FairShare/Output.txt", "a")
        text_file.write('Writing data to Excel \n')      
        
        print('Returned data is of type ' + str(type(data)))
        
        data.to_excel(writer, sheet_name='Raw Converted', index=False)  
         
        for column in data:
            column_width = max(data[column].astype(str).map(len).max(), len(column))
            col_idx = data.columns.get_loc(column)
            writer.sheets['Raw Converted'].set_column(col_idx, col_idx, column_width)
            
        writer.save()
        
        docName = "Roundys_Fairshare_{}.xlsx".format(UploadCode)
    
    for doc in docs:
        shutil.move(doc, "./Roundy_FairShare/Processed/" + doc.split("\\")[1])
        print('Moving ' + doc + ' to processed folder.')
    text_file.write('PDFs Moved to Proccessed Folder \n')

    # opening EXCEL through Code
    #local path in dir
    absolutePath = Path('./{}'.format(docName)).resolve()
    os.system(f'start excel.exe "{absolutePath}"')
    
    text_file.write('Conversion Complete')
    text_file.close()
    print('Conversion completed, please close this window.')