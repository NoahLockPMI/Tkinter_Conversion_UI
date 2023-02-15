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
import re
import warnings

#os.system('cls' if os.name == 'nt' else 'clear')

def process_doc(doc):
    #dynamic conversion approach
    
    #define rules for static data
    def invoiceRule(df, posX, posY):
        return df[posX][posY+2]
    def chargebackRule(df, posX, posY):
        return df[posX][posY]
    def scanTotalRule(df, posX, posY):
        return df[df.shape[1]-1][posY]
    def epFeeRule(df, posX, posY):
        return df[df.shape[1]-1][posY]
    def invoiceTotalRule(df, posX, posY):
        return df[df.shape[1]-1][posY]
    
    #define rules for tabled data
    def upcNumRule(row):
        return getLeftMost(row,1)
    def brandRule(row):
        newStr, newRow = getLeftMost(row,1)
        if newStr == 'ENJOY':
            secondStr, newRow = getLeftMost(row,1)
            newStr += ' ' + secondStr
        return newStr, newRow
    def poNumRule(row):
        return getRightMost(row,1)
    def amountRule(row):
        return getRightMost(row,1)
    def unitsRule(row):
        return getRightMost(row,1)
    def dateEndRule(row):
        return getRightMost(row,1)
    def dateStartRule(row):
        return getRightMost(row,1)
    def reasonRule(row):
        return getRightMost(row,2) 
    def bannerRule(row):
        return getRightMost(row,3)
    def productDescriptionRule(row):
        return getLeftRemaining(row)
    
    #define general data processing
    def cleanData(row):
        for i in range(row.size):
            if type(row[i]) == type(''):
                row[i] = str(row[i]).replace("N DATA REPORTDY'S WISCO", "DATA REPORT ")
                row[i] = str(row[i]).replace("N DATA REPORTDY'S CHIC", "DATA REPORT ")
                row[i] = str(row[i]).replace("N DATA REPORTNDY'S WISCO", "DATA REPORT ")
                row[i] = str(row[i]).replace("N DATA REPORTNDY'S CHIC", "DATA REPORT ")
    
    def invalidProcessing(row):
        newStr, newRow = getLeftRemaining(row)
        tableDatum[8].data.iloc[-1] += ' ' + str(newStr)

    def rowIsValid(row):
        return not(pd.isna(row[len(row) - 1]))

    #define phase processes (shouldn't need to change much)
    def headerProcess(df, row):
        for index in range(len(row)):
            if staticDatum[staticDataIndex].checkTrigger(str(df[index][row.name])):
                staticDatum[staticDataIndex].setValue(df, index, row.name)
    
    def bodyProcess(df, row):
        #print('before: ' + ' '.join(row.apply(str).values))
        cleanData(row)
        if rowIsValid(row):
            for field in tableDatum:
                tempRow = pd.Series.copy(row)
                newStr, row = field.rule(row)
                if field.isDataValid(newStr):
                    newSeries = pd.Series(newStr)
                    field.addData(newSeries)
                else:
                    row = tempRow
                    field.addData(pd.Series(''))
        else:
            invalidProcessing(row)
    
    def footerProcess(df, row):
        for index in range(len(row)):
            if staticDatum[staticDataIndex].checkTrigger(str(df[index][row.name])):
                staticDatum[staticDataIndex].setValue(df, index, row.name)

    #helper classes
    class Phase:
        def __init__(self, processMethod, triggerCondition):
            self.processMethod = processMethod
            self.triggerCondition = triggerCondition
            self.trigger = False
        def checkTrigger(self, condition):
            self.trigger = condition == self.triggerCondition or self.trigger
        def processRow(self, df, row):
            for column in row:
                self.checkTrigger(str(column))
            if not(self.trigger):
                self.processMethod(df, row)

    class TableData:
        def __init__(self, name, rule, pattern):
            self.name = name
            self.rule = rule
            self.pattern = re.compile(pattern)
            self.data = pd.Series(dtype=str)
        def addData(self, newStr):
            self.data = pd.concat([self.data,newStr])
        def isDataValid(self, field):
            if field == math.inf:
                return False
            patternMatch = self.pattern.match(field)
            return field == '' or patternMatch != None

    class StaticData:
        def __init__(self, name, rule, triggerCondition):
            self.name = name
            self.rule = rule
            self.triggerCondition = triggerCondition
            self.trigger = False
            self.value = ''
        
        def checkTrigger(self, condition):
            conditionSearch = re.search(self.triggerCondition,condition)     
            return conditionSearch != None

        def setValue(self, df, posX, posY):
            self.trigger = True
            self.value = self.rule(df, posX, posY)

    #helper functions
    def getLeftRemaining(row):
        leftValue = row.iloc[0]
        while(leftValue == math.inf or leftValue != leftValue):
            row = row[1:]
            leftValue = row.iloc[0]
        return leftValue, row
    
    def getLeftMost(row, n):
        leftValue = row.iloc[0]
        while(leftValue == math.inf):
            row = row[1:]
            leftValue = row.iloc[0]
        if leftValue != leftValue:
            return '', row[1:]
        else:
            choppedValue, retExtraction = getLeftValue(str(leftValue),n)
            if choppedValue == '':
                if len(row) == 1:
                    return math.inf, pd.Series()
                else:
                    row = row[1:]
            else:
                row.iloc[0] = choppedValue
            return retExtraction, row
    
    def getRightMost(row,n):
        rightValue = row.iloc[-1]
        while(rightValue == math.inf):
            row = row[:-1]
            rightValue = row.iloc[-1]
        if rightValue != rightValue:
            return '', row[:-1]
        else:
            choppedValue, retExtraction = getRightValue(str(rightValue),n)
            if choppedValue == '':
                if len(row) == 1:
                    return math.inf, pd.Series()
                else:
                    row = row[:-1]
            else:
                row.iloc[-1] = choppedValue
            return retExtraction, row

    def getLeftValue(value, n):
        leftMost = ''
        for i in range(n):
            split = getLeftDelimeter(value)
            if split == -1:
                return '', str.strip(value + ' ' + leftMost)
            else:
                leftMost = splitLeft(value, split) + ' ' + leftMost
                value = splitRight(value, split+1)
        return value, str.strip(leftMost)
    
    def getRightValue(value, n):
        rightMost = ''
        for i in range(n):
            split = getRightDelimeter(value)
            if split == -1:
                return '', str.strip(value + ' ' + rightMost)
            else:
                rightMost = splitRight(value, split+1) + ' ' + rightMost
                value = splitLeft(value, split)
        return value, str.strip(rightMost)
    
    def getLeftDelimeter(field):
        return str.strip(field).find(' ',0)
    def getRightDelimeter(field):
        return str.strip(field).rfind(' ',0)
    def splitLeft(field, split):
        return field[0:split]
    def splitRight(field, split):
        return field[split:]

    # preprocessing
    def checkAllNan(column):
        firstField = column[0]
        try:
            a = math.isnan(firstField)
            b = len(column.unique()) == 1
        except:
            a = False
            b = False
        return a and b
    
    warnings.filterwarnings(action='ignore', category=FutureWarning)

    print('Converting this doc ' + doc)
    pdf = doc
    text_file = open("./Roundy_Scans/Output.txt", "a")
    text_file.write('Processing: %s\n' % pdf)

    #pdfObj = open(pdf, 'rb')
    #reader = PyPDF2.PdfFileReader(pdfObj, strict=False)
    pd.set_option('display.max_rows', 500)
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    pages = tabula.read_pdf(pdf, pages='all', pandas_options={'header': None}, guess=False, silent=True)

    # for page in pages:
    #     print(page)

    # preprocessing
    text_file.write('Processing all pages for Nan\n')
    for page in pages:
        for i in range(page.shape[1]):
            if checkAllNan(page[i]):
                page[i] = math.inf
    
    text_file.write('Phases begin recording\n')
    phases = []
    phases.append(Phase(headerProcess,'UPC # Product Information'))
    phases.append(Phase(bodyProcess,'All Inquires Should Be Emailed To  :'))
    phases.append(Phase(footerProcess,None))
    text_file.write('Phases recorded\n')
    
    text_file.write('StaticDatum begin recording\n')
    staticDatum = []
    staticDatum.append(StaticData('Invoice#',invoiceRule,'Invoice #'))
    staticDatum.append(StaticData('Chargeback / Invoice',chargebackRule,"Roundy's Scan Invoice"))
    staticDatum.append(StaticData('Scan Total',scanTotalRule, 'Scan Total:'))
    staticDatum.append(StaticData('EP Fee',epFeeRule,'EP Fee :'))
    staticDatum.append(StaticData('Invoice Total',invoiceTotalRule, 'Invoice Total:'))
    text_file.write('StaticDatum recorded\n')

    text_file.write('TableDatum begin recording\n')
    tableDatum = []
    tableDatum.append(TableData('UPC#',upcNumRule,r'\b\d{12}\b'))
    tableDatum.append(TableData('Brand',brandRule,r'\b\w+\b'))
    tableDatum.append(TableData('PO#',poNumRule,'^[^-]*-[^-]*-[^-]*$'))
    tableDatum.append(TableData('Amount',amountRule,'^\$[0-9]+(\.[0-9]{2})?$'))
    tableDatum.append(TableData('Units',unitsRule,r'\d+(\.\d+)?'))
    tableDatum.append(TableData('Date End',dateEndRule,'^(0[1-9]|1[0-2])/(0[1-9]|[1-2][0-9]|3[0-1])/[0-9]{4}$'))
    tableDatum.append(TableData('Date Start',dateStartRule,'^(0[1-9]|1[0-2])/(0[1-9]|[1-2][0-9]|3[0-1])/[0-9]{4}$'))
    tableDatum.append(TableData('Reason',reasonRule,r'^(FCB Scan|DATA REPORT)$'))
    tableDatum.append(TableData('Banner',bannerRule,r'\b\w+\b \- \b\w+\b'))
    tableDatum.append(TableData('Product Description',productDescriptionRule,r'\b\w+\b'))
    text_file.write('TableDatum recorded\n')

    phaseIndex = 0
    staticDataIndex = 0

    body = pd.DataFrame()
    
    text_file.write("Data clean up started\n")
    for page in pages:
        for index, row in page[:-1].iterrows():
            phases[phaseIndex].processRow(page,row)
            if phases[phaseIndex].trigger:
                phaseIndex += 1
            if staticDatum[staticDataIndex].trigger and staticDataIndex + 1 < len(staticDatum):
                staticDataIndex += 1

    for d in tableDatum:
        body[d.name] = d.data.reset_index(drop=True)
    
    for data in staticDatum:
        body[data.name] = data.value
    text_file.write("Data cleanup finished\n")
    body['DC#'] = 18
    body = body[['Invoice#','DC#','Chargeback / Invoice','UPC#','Brand','Product Description','Banner', 'Reason', 'Date Start', 'Date End', 'Units','Amount', 'PO#', 'Scan Total', 'EP Fee', 'Invoice Total']]
    text_file.write("Body headers labeled \n\n")
    text_file.close()
    # print(body)

    return body

def startConversion(docs):
    # On Windows calling this function is necessary.
    multiprocessing.freeze_support()
    #subprocess.Popen(['java', '-jar', 'tabula-1.0.5-jar-with-dependencies.jar'])
    #time.sleep(5)
    path = os.environ['PATH']

    if 'Java\jre' not in path:
        print('Path Does not Exist')
        os.environ['PATH'] = "{}{}{}".format('C:/Program Files (x86)/Java/jre1.8.0_281/bin', os.pathsep, path)
    else:
        print('Exist')
    
    # docs = glob.glob("*.pdf")
    
    UploadCode = str(uuid.uuid4()).upper()

    debugging = False
    debug = [
        #'RN84033818 - RN84044918_18bc.pdf'
        'RN64083418 - RN64093818_25bc.pdf'
        #'RN64070318 - RN64083318_31bc.pdf'
        #'RN64058218 - RN64070218_24bc.pdf'
        #'RN64049318 - RN64053518_14bc.pdf'
        #'RN43993218 - RN44002518_15bc.pdf'
        #'RN23975118 - RN23983718_13bc.pdf'
        #'RN23965218 - RN23975018_16bc.pdf'
        #'RN23953818 - RN23965118_19bc.pdf'
        #'RN23941218 - RN23953718_21bc.pdf'
        #'RN23928818 - RN23941118_23bc.pdf'
        #'RN23918118 - RN23928718_17bc.pdf'
        #'RN23908118 - RN23918018_17bc.pdf'
        #'RN23667818 - RN23679118_2bc  372.38.pdf'
        #'RN23641318 - RN23653718_3bc  1573.43.pdf'
        ]
    
    print('Beginning conversion process.')
    text_file = open("./Roundy_Scans/Output.txt", "w")
    text_file.write('Beginning conversion process. \n\n')
    text_file.close()
            
    data = pd.DataFrame()
    
    with Pool(4) as p:
        if debugging:
            data = pd.concat(p.map(process_doc, debug))  
        else:
            data = pd.concat(p.map(process_doc, docs))

        writer = pd.ExcelWriter('Roundys_Scans_{}.xlsx'.format(UploadCode), engine='xlsxwriter')         
        
        print('Returned data is of type ' + str(type(data)))

        text_file = open("./Roundy_Scans/Output.txt", "a")
        text_file.write('Writing data to Excel \n')

        data.to_excel(writer, sheet_name='Raw Converted', index=False)  
         
        for column in data:
            column_width = max(data[column].astype(str).map(len).max(), len(column))
            col_idx = data.columns.get_loc(column)
            writer.sheets['Raw Converted'].set_column(col_idx, col_idx, column_width)
            
        writer.save()
        
        docName = "Roundys_Scans_{}.xlsx".format(UploadCode)
    
    for doc in docs:
        shutil.move(doc, "./Roundy_Scans/Processed/" + doc.split("\\")[1])
        print('Moving ' + doc + ' to processed folder.\n') 
    text_file.write('PDFs moved to Processed folder\n')
    # opening EXCEL through Code
    #local path in dir
    absolutePath = Path('./{}'.format(docName)).resolve()
    os.system(f'start excel.exe "{absolutePath}"')

    text_file.write('Conversion Complete')
    text_file.close()
    print('Conversion completed, please close this window.')


# if __name__ == "__main__":
#     docs = glob.glob("./Roundy_Scans/*.pdf")
#     startConversion(docs)