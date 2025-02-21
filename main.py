import os
import re
import csv
import pandas as pd
import traceback
import mysql.connector
from datetime import datetime, timedelta
from dotenv import load_dotenv
from collections import defaultdict
import time
load_dotenv() 
def convertDate(date):
  date_obj =datetime.strptime(date, '%m/%d/%Y') 
  return date_obj 


def connectionDb():  
  try:
    mydb = mysql.connector.connect(
      host = os.getenv('dbhostTest'),
      user = os.getenv('userdbTest'),
      passwd = os.getenv('passdbTest'),
      database = os.getenv('dbnameTest'),
      auth_plugin = 'mysql_native_password',      
    )
    
    return mydb
  except:    
    
    return('An error occurred on the database connection!')  
def checkAccount(card):
  try:
    
    mydb = connectionDb()
    myCursor = mydb.cursor()
    sqlScript = "SELECT id FROM cards WHERE name LIKE %s;"
    myCursor.execute(sqlScript, (f"%{card}%",))
    result = myCursor.fetchone()
    if result == None:
      return False
    mydb.close()
    return result[0]
  except:
    return('An error occurred checking the cards')
  
  
def registerTransaction(rows_to_insert):   
  
  
  
  try:
    mydb = connectionDb()
    myCursor = mydb.cursor()
    query = '''
            INSERT INTO movement (card_id, payment_date, payment_description, payment_ammount)
            VALUES (%s, %s, %s, %s)
        '''
    myCursor.executemany(query, rows_to_insert)
    mydb.commit()
    mydb.close()
  except:
    return('An error occurred registering movement of ther cards')
    

def checkMovementDb(date, account_id, description, amount, count):
  
  
  try: 
    
    mydb = connectionDb()
    myCursor = mydb.cursor()   
    
    query = '''
            SELECT COUNT(*) 
            FROM movement 
            WHERE card_id = %s AND payment_date = %s AND payment_description = %s AND payment_ammount = %s
        '''
    myCursor.execute(query, (account_id, date, description, amount))
    actual_count = myCursor.fetchone()[0]
      
    
    myCursor.close()    
    if actual_count < count:
        # Calcular cuántas transacciones faltan
      missing_count = count - actual_count
      rows_to_insert = [
        (account_id, date, description, amount) for _ in range(missing_count)
      ]
      
     

      # Inserción masiva
      # query_insert = '''
      #     INSERT INTO movement (card_id, payment_date, payment_description, payment_ammount)
      #     VALUES (%s, %s, %s, %s)
      # '''
      # myCursor.executemany(query_insert, rows_to_insert)
      # mydb.commit()
      # mydb.close()
      
      
      # for _ in range(missing_count):
      registerTransaction( rows_to_insert)
       
        
    
    else:
      return False
  except:
    return('An error occurred checking movement of ther cards')
  
  
  
def proccessTransaction(data, account_id):
  parser_data = []
  transaction = defaultdict(lambda: {'count': 0, 'amount': 0, 'date': None})
  
  
  
  try:
    for row in data:
      
      date = row[0]
      description = row[1]
      amount = row[2]
      
      
      key = (description, amount, date)
      transaction[key]['count'] += 1
      transaction[key]['amount'] = amount 
      transaction[key]['date'] = date
      
    
    
    for key, value in transaction.items():
      transaction_ordered ={
        'transaction':{
          'description': key[0],
          'amount': key[1], 
          'date': key[2],
        },
        'count': value['count'],
      }
      parser_data.append(transaction_ordered)
      
    
    
    
    for item in parser_data:   
     
           
      
      date = item['transaction']['date']
      description = item['transaction']['description']
      ammount = item['transaction']['amount']
      count = item['count']     
      
      
      
      
      checkMovementDb(date, account_id, description, ammount, count)
    
    
    
    
    
  except Exception as e:
    print("Se produjo un error:")
    traceback.print_exc()


def ReadCheckingOrSaving(data , account_id):
  try:
    parser_data = []
    for row in data:
      date =convertDate(row[0])
      description = row[10]
      if row[3].lower() == 'credit':
        amount = float(row[2])
      elif row[3].lower() == 'debit':
        amount = float(row[2]) * -1
      parser_data.append([date, description, amount])
      
    
      
    proccessTransaction(parser_data, account_id)
        
    
  except Exception as e:
    print("Se produjo un error:")
    traceback.print_exc()
  
def redDataCaixa(data , account_id):
  try:
    parser_data = []
    for row in data:  
      date = convertDate(row[0])
      description = row[2]
      amount = row[3]
      parser_data.append([date, description, amount])
    
    
      
    proccessTransaction(parser_data, account_id)
      
   
    
    
  except Exception as e:
    print("Se produjo un error:")
    traceback.print_exc() 
def check_folder(path):
  try:
    files = os.listdir(path)
    for file in files:
      path_file = os.path.join(path, file)
      data =[]
      if os.path.isfile(path_file) and path_file.endswith(('.xls', '.XLS', 'XLSX', 'xlsx')):
        if "movimientos" in file.lower():
          account = "CAIXA"
          #account_id = checkAccount(account)
          
          df = pd.read_excel(path_file,engine='openpyxl')
          originalname = os.path.splitext(path_file)[0]
          
          csvName = f"{originalname}.csv"  
          df.to_csv(csvName, index=False)
          with open(csvName, mode='r') as movement_file:
            reader = csv.reader(movement_file)
            for row in reader:                
              data.append(row)
              
          # name = data[0][1].split(" ")[0]
          # account = f"{account} {name.upper()}"
          # account_id = checkAccount(account)
          # print(account)
          del data[0:3]
          
          #print(data)
          redDataCaixa(data, account_id)
          os.remove(csvName)
          os.remove(path_file)
      elif(os.path.isfile(path_file) and path_file.endswith(('.csv', '.CSV'))):
        if "checking" in file.lower()or 'saving' in file.lower():
          account = os.path.splitext(file)[0].upper()
          account = account.replace(" ", "%")
          account_id = checkAccount(account)
          print(account)
          print(account_id)
          with open(path_file, mode='r') as movement_file:
              reader = csv.reader(movement_file)
              for row in reader:
                data.append(row)
              del data[0]
              
              
              ReadCheckingOrSaving(data , account_id)
        
          
          
        
        
  except Exception as e:
    print("Se produjo un error:")
    traceback.print_exc() 
check_folder(r"./files")