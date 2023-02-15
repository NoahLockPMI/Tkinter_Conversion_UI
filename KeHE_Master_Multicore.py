#Azure SQL Liberies 
import sqlalchemy
import sqlalchemy.sql.default_comparator
from sqlalchemy import create_engine
import pyodbc
import urllib.parse
from urllib.parse import quote_plus, scheme_chars
from openpyxl import load_workbook
import threading
import numpy as np
import uuid
#Conversion Liberies 
import glob
import pandas as pd
import shutil
import math
import time
import tika 
tika.initVM()
from tika import parser
import re
import multiprocessing
import multiprocessing.forkserver
from multiprocessing import Pool  
import subprocess
from pathlib import Path
import os 
import sys

#tika is a raw PDF parser library
#Convert the doc to raw string
#Slice & find data we need
#Pop data into df 
#Either export or upload to DB

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

def find_string(txt, str1):
    return txt.find(str1, txt.find(str1)+1)

def assign_division(txt):
    switcher = {
        'Division:  1': "Rocklin CA",
        'Division:  2': "Seattle WA",
        'Division:  3': "default",
        'Division:  4': "Moreno Valley CA",
        'Division:  5': "Denver CO",
        'Division:  6': "Portland WA",
        'Division:  7': "Lancaster TX",
        'Division:  8': "Gilroy CA"
    }
    return switcher.get(txt,"default") 

states = ["AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DC", "DE", "FL", "GA", 
          "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", 
          "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ", 
          "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC", 
          "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY"]

def process_doc(doc): 
    # print('Process has started for ' + doc)
    results = pd.DataFrame()

    text_file = open("./KeHE_Master_Multicore/Output.txt", "a")
    text_file.write('Processing: %s\n' % doc)

    # print('file name: ' + doc)
    #final results   
    #grab doc & extract raw data 
    text_file.write('Getting Table Header\n')
    raw = parser.from_file(doc, requestOptions={'timeout': 120})
    data = str(raw['content']).replace('\n\n', '\n')
    product_row_header = '   UPC #     SHIP      DESCRIPTION     NBR     DATE  COMMENT      COST    $ OR %  EXT-COST'
    product_row_start = '------------ ----- ---------------- ---------------  --------- --------  -------  --------'
    skip_rows = [product_row_header, product_row_start]
    header_location = data.find('====================================================================================')
    header_data = data[data.find('INVOICE'):header_location]
    core_data = data[data.find('===================================================================================='):]
    payee_location = data[data.find('PAYEE:'):header_location].splitlines()[1]
    temp = re.findall(r'\d+', payee_location)
    payee_number = list(map(int, temp))
    payee_number = str(payee_number).replace('[', '').replace(']', '')    
    # print(payee_location)
    focused_row = 2
    row = 1    
    invoice_number = header_data.splitlines()[0].split()[1].replace('#', '')
    invoice_number = invoice_number + '_' + payee_number
    # print(invoice_number)
    distributor = header_data.splitlines()[1]
    distributor = distributor[distributor.find('KEHE'):].replace('KEHE DIST - ', '')
    total_fee = header_data.splitlines()[5]
    total_fee = " ".join(total_fee[total_fee.find('TOTAL FEE'):].split()).replace('TOTAL FEE', '').replace(' ', '')
    vendor_start = header_data.splitlines()[3].find(':') + 1
    totalmcb_start = header_data.splitlines()[3].find('TOTAL') - 6
    vendor_name = header_data.splitlines()[3][vendor_start:totalmcb_start].strip()
    vendor_number = header_data.splitlines()[4].split()[0]
    
    beginning_of_customer = False   
    end_of_customer = False
    is_product_row = False    
    customer_name = ''
    customer_address = ''
    text_file.write('Header Table Recorded \n')

    if total_fee.strip() != '':        
        row_data = [{'Doc':doc, 'UPC': '', 'Shipped': '','Description': 'FEE','ReferenceNumber': '','Date': '', 'Comment': '','Cost': '', 'Discount': '','ExtCost': total_fee, 'Percent': '', 'Dollar': '', 'InvoiceNumber': invoice_number,'Distributor': distributor,'SoldTo': '','CustomerName': '','CustomerAddress': ''}]
        df = pd.DataFrame(row_data)
        results = results.append(df, ignore_index=True)


        # print('Row' + str(row) + ':' + line)    
    text_file.write('Setting up data\n')     
    for line in core_data.splitlines():  
            # print('Row' + str(row) + ':' + line)         
        if row == focused_row:  
            text_file.write('Setting up Row: %s\n' % row)     
            skipped = False          
            if 'SOLD TO:' in line:
                beginning_of_customer = True                                    
                customer_name = line[line.find('SOLD TO:')+9:].strip()                    

                customer_address = core_data.splitlines()[row]
                customer_address = customer_address[:customer_address.find('TOL') - 4].strip()
                sold_to = core_data.splitlines()[row + 1].split()[0]
                customer_citystate = ''
                customer_address2 = ''
                if customer_address == customer_name:    
                    # print('Found duplicate : ' + customer_address)
                    customer_address = ' '.join(core_data.splitlines()[row + 1].split()[1:])
                     #print('Removed vendor number : ' + customer_address)
                    customer_address = customer_address[:customer_address.find('TELE')].strip()   
                    # print('Finding real address: ' + customer_address)
                    customer_address1 = " ".join(core_data.splitlines()[row + 2].split())                      
                    # print('other looking for state and city : ' + customer_address1)                 
                else:
                    customer_address1 = core_data.splitlines()[row + 1]                    
                    customer_address1 = customer_address1[customer_address1.find(sold_to) + len(str(sold_to)):customer_address1.find('TELE') - 5].strip()
                    customer_address2 = core_data.splitlines()[row + 2]                    
                customer_address = customer_address + ' ' + customer_address1  
                customer_citystate = " ".join(customer_address1.split()) 
                # customer_citystate = customer_address.split()[-3] + ' ' + customer_address.split()[-2] 
                # print(customer_citystate)     
                if 'QTY' not in customer_address2:
                    # print('Pre fixed address: ' + customer_address)
                    # print('QTY not in address 2' + customer_address2)
                    customer_citystate = " ".join(customer_address2.split()[:-1])
                    customer_address2 = " ".join(customer_address2.split())
                    customer_address = customer_address + ' ' + customer_address2
                    # print('Fixed  Address: ' + customer_address)
                customer_address = " ".join(customer_address.split())                
                # print('doc :' + doc)
                # print(customer_name)
                # print(customer_citystate)
                # print('Pre address 1: ' + customer_address1)
                # print('Pre address 2: ' + customer_address2)
                if len(customer_citystate.split()) > 1:
                    if customer_citystate.split()[-1].isdigit():
                        if 'QTY' not in customer_address2:
                            customer_citystate = " ".join(customer_address2.split()[:-2]) + ', ' + customer_address2.split()[-2]   
                        else:
                            customer_citystate = " ".join(customer_address1.split()[:-2]) + ', ' + customer_address1.split()[-2]   
                    else:
                        customer_citystate = " ".join(customer_citystate.split()[:-1]) + ', ' + customer_citystate.split()[-1]
 
                if customer_citystate != '' and len(customer_citystate.strip()) > 1: 
                    if customer_citystate.strip() == 'GRAND' or customer_citystate.strip() == 'GRAND CAYMAN CAYMA..' or customer_citystate.strip() == 'GRAND CAYMAN, CAYMA':
                        customer_citystate = 'CAYMAN ISLANDS'                    
                    if '..' in customer_citystate:
                        customer_citystate = customer_citystate.replace('..', '')
                    if customer_citystate.strip()[-1] == ',':
                        customer_citystate = customer_citystate.replace(",", "")
                    last_two_char = customer_citystate[-2:]
                    if last_two_char in states:
                        customer_citystate = customer_citystate[:-2].replace(', ', ' ') + ', ' + customer_citystate[-2:]
                        customer_citystate = customer_citystate.replace(' , ', ', ')
                if customer_citystate.strip() == 'GRAND CAYMAN, CAYMA' or customer_citystate.strip() == 'GRAND CAYMAN CAY, MA':
                        customer_citystate = 'CAYMAN ISLANDS'                
                # print(customer_citystate)  
                # print('final customer: ' + customer_address)
                # print('customer name: ' + customer_name)
                # print('customer address: ' + customer_address)
                # print('sold to: ' + sold_to)
            elif product_row_header in line:
                skipped = True
            elif product_row_start in line:
                skipped = True
            elif 'INVOICE' in line or 'Date' in line or 'DC' in line or 'Page' in line:
                skipped = True
            elif line.strip() == '':
                skipped = True
            elif line.split()[0].isdigit() and len(str(line.split()[0])) == 12:
                is_product_row = True
                product_row = line
                date_location = 0
                for item in product_row.split(): 
                    if re.search('/.+/', item):
                        if len(item) > 9:
                            combinedfield = product_row.split()[date_location]
                            # print('Combined field: ' + combinedfield)
                            date = combinedfield[-8:]
                            # print('date field: ' + date)
                            reference_number = combinedfield[:-8]
                            # print('reference number: ' + reference_number)
                            line_replacement = product_row.split()
                            line_replacement.remove(combinedfield)
                            line_replacement.insert(date_location, reference_number)
                            line_replacement.insert(date_location + 1, date)
                            product_row = ' '.join(line_replacement)
                            # print('Fixed line: ' + product_row)
                            date_location += 1
                        break
                    date_location += 1
                ext_cost = product_row.split()[-1]
                if product_row.split()[-2] == '%':                        
                    discount = product_row.split()[-3] + product_row.split()[-2] 
                    cost = product_row.split()[-4]
                    comment = " ".join(product_row.split()[date_location + 1:-4])
                else:
                    discount = product_row.split()[-2]
                    cost = product_row.split()[-3]
                    comment = " ".join(product_row.split()[date_location + 1:-3])
                upc = product_row.split()[0]
                shipped = product_row.split()[1]
                description = " ".join(product_row.split()[2:date_location - 1])
                reference_number = " ".join(product_row.split()[date_location - 1]).replace(' ', '')
                date = " ".join(product_row.split()[date_location]).replace(' ', '')
                dollar = float(ext_cost) / float(shipped)
                # rounded_dollar = math.ceil(float(dollar) * 100.0) / 100.0
                # rounded_cost = math.ceil(float(cost) * 100.0) / 100.0
                # rounded_dollar = round(float(dollar), 3) 
                # rounded_cost = round(float(cost), 3) 
                # percent = rounded_dollar / rounded_cost
                
                percent = float(dollar) / float(cost)
                percent = str(float(round(percent * 100))) + '%'
                #print('upc: ' + upc)
                # print('shipped: ' + shipped)
                # print('description: ' + description)
                # print('reference number: ' + reference_number)
                # print('ext_cost: ' + ext_cost)
                # print('discount: ' + discount)
                # print('cost: ' + cost)
                # print('comment: ' + comment)
                # print('Product Row: ' + line)    
                # 'CustomerCityState': customer_citystate            
                row_data = [{'Doc':doc, 'UPC': upc, 'Shipped': shipped,'Description': description,'ReferenceNumber': reference_number,'Date': date, 'Comment': comment,'Cost': cost, 'Discount': discount,'ExtCost': ext_cost, 'Percent': percent, 'Dollar': dollar, 'InvoiceNumber': invoice_number,'Distributor': distributor,'SoldTo': sold_to,'CustomerName': customer_name,'CustomerAddress': customer_address, 'CustomerCityState': customer_citystate }]
                df = pd.DataFrame(row_data)
                results = results.append(df, ignore_index=True)                         
            focused_row +=1    
        row +=1

    print('Successfully Converted ' + doc)
    text_file.write('Finished %s\n\n' % doc)
    text_file.close()
    return results 

data = None

def startConversion(docs):  
    # On Windows calling this function is necessary.    
    multiprocessing.freeze_support()     
    # subprocess.Popen(['java', '-jar', 'tika-server.jar'])
    # time.sleep(5)
    # os.environ['TIKA_CLIENT_ONLY'] = 'False'
    # os.environ['TIKA_STARTUP_SLEEP'] = '30'
    path = os.environ['PATH']

    if 'Java\jre' not in path:
        print('Path Does not Exist')
        os.environ['PATH'] = "{}{}{}".format('C:/Program Files (x86)/Java/jre1.8.0_201/bin', os.pathsep, path)
    else:
        print('Exist')

    # docs = glob.glob("*.pdf")
    debug = ['0046092_23008492.pdf']  
    UploadCode = str(uuid.uuid4()).upper()
    
    print('Beginning conversion process.')  
    text_file = open("./KeHE_Master_Multicore/Output.txt", "w")
    text_file.write('Beginning conversion process. \n\n')
    text_file.close()
        
    with Pool(4) as p:        
        data = pd.concat(p.map(process_doc, docs)).reset_index(drop=True)
               
        writer = pd.ExcelWriter('KeHE_UploadResults_{}.xlsx'.format(UploadCode), engine='xlsxwriter')         
        
        data.to_excel(writer, sheet_name='Raw Converted', index=False)  
         
        for column in data:
            column_width = max(data[column].astype(str).map(len).max(), len(column))
            col_idx = data.columns.get_loc(column)
            writer.sheets['Raw Converted'].set_column(col_idx, col_idx, column_width)
            
        writer.save()
        
        docName = "KeHE_UploadResults_{}.xlsx".format(UploadCode)
        text_file = open("./KeHE_Master_Multicore/Output.txt", "a")
        text_file.write('Writing data to Excel \n')

    for doc in docs:
        shutil.move(doc, "./KeHE_Master_Multicore/Processed/" + doc.split("\\")[1])
        print('Moving ' + doc + ' to processed folder.')
    text_file.write('PDFs Moved to Proccessed Folder \n')

    # opening EXCEL through Code
    #local path in dir
    absolutePath = Path('./{}'.format(docName)).resolve()
    os.system(f'start excel.exe "{absolutePath}"')
    text_file.write('Conversion Complete')
    text_file.close()

    print('Conversion completed, please close this window.')
