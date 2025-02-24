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

#funtion to load the .env data
load_dotenv() 

# Function to convert date format from mm/dd/yyyy to python datetime object
def convertDate(date):
  date_obj =datetime.strptime(date, '%m/%d/%Y') 
  return date_obj 


#Function to connect to the database
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
  
#funtion to check the account in the database and get the ID
# GREG: used to be checkAccount(card)
def getCardIdFromCardName(card):
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
  
  
  
#Function to register the transaction in the database
def registerTransaction(rows_to_insert):     
  try:
    mydb = connectionDb()
    myCursor = mydb.cursor()
    query = '''
            INSERT INTO movement (card_id, payment_date, payment_description, payment_amount)
            VALUES (%s, %s, %s, %s)
        '''
    myCursor.executemany(query, rows_to_insert)
    mydb.commit()
    mydb.close()
  except:
    return('An error occurred registering movement of the cards')
  
  
#funtion to check the transaction on the database (I should to improve performance)
def checkMovementDb(date, account_id, description, amount, count): 
  try: 
    #check the transaction and the times of the repeat of the transaction on the database.
    mydb = connectionDb()
    myCursor = mydb.cursor()   
    
    query = '''
            SELECT COUNT(id) 
            FROM movement 
            WHERE card_id = %s AND payment_date = %s AND payment_description = %s AND payment_amount = %s
        '''
    myCursor.execute(query, (account_id, date, description, amount))
    actual_count = myCursor.fetchone()[0] 
    print(actual_count)
    myCursor.close()    
    
    #if the result  < of the variable
    if actual_count < count:
      # Check how much the transaction are repeat 
      missing_count = count - actual_count
      rows_to_insert = [
        (account_id, date, description, amount) for _ in range(missing_count)
      ]
      #registerTransaction( rows_to_insert)
    else:
      return False
  except:
    return('An error occurred checking movement of the cards')
  
  
#funtion to order all transactions before checking the database and compare
def processTransaction(data, account_id):
  parser_data = []
  #create a dictionary with a count variable to check the transaction times existing in the file
  transaction = defaultdict(lambda: {'count': 0, 'amount': 0, 'date': None})
  
  try:
    #loop to check the transaction if is repeatable on the file if exists we +1 on the count variable
    for row in data:
      
      date = row[0]
      description = row[1]
      amount = row[2]
      
      key = (description, amount, date)
      transaction[key]['count'] += 1
      transaction[key]['amount'] = amount 
      transaction[key]['date'] = date
      
    #Order the dictionary by transaction
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
     #loop to check each transaction on the database
      date = item['transaction']['date']
      description = item['transaction']['description']
      amount = item['transaction']['amount']
      count = item['count']     
      
      checkMovementDb(date, account_id, description, amount, count)
    
    
  except Exception as e:
    print("An error occurred:")
    traceback.print_exc()




#function to read data on files with checking or saving in the name
def ReadCheckingOrSaving(data , account_id):
  try:
    parser_data = []
    #erxtract date, description, and amount from de data of csv file
    for row in data:
      date =convertDate(row[0])
      description = row[10]
      if row[3].lower() == 'credit':
        amount = float(row[2])
      elif row[3].lower() == 'debit':
        amount = float(row[2]) * -1
      parser_data.append([date, description, amount])
      
    processTransaction(parser_data, account_id)
        
  except Exception as e:
    print("An error occurred:")
    traceback.print_exc()
  
#Function to read data from caixa bank
def redDataCaixa(data , account_id):
  try:
    parser_data = []
    #erxtract date, description, and amount from de data of csv file
    
    for row in data:  
      date = convertDate(row[0])
      description = row[2]
      amount = row[3]
      parser_data.append([date, description, amount])
    
     
    processTransaction(parser_data, account_id)
      
   
  except Exception as e:
    print("An error occurred:")
    traceback.print_exc() 

#function to get ID from File    
def getBankIdFromFile(file, property=None):
  if property:
    account = f"{file}%{property.upper()}"    
    return getCardIdFromCardName(account)
  else:
    account = os.path.splitext(file)[0].upper()
    account = account.replace(" ", "%")
    return getCardIdFromCardName(account)
    
    
  
#funtion to check files on the folder
def check_folder(path):
  try:
    #list all the files
    files = os.listdir(path)
    for file in files:
        
        #GREG INSERT:
        
      #read the files
      path_file = os.path.join(path, file)
      data =[]
      if os.path.isfile(path_file) and path_file.endswith(('.xls', '.XLS', 'XLSX', 'xlsx')):
        # process if the file has the word 'movimientos' in the name.
        if 'movimientos' in file.lower(): 
          #read and extract the data
          df = pd.read_excel(path_file,engine='openpyxl')          
          originalName = os.path.splitext(path_file)[0]  
          csvName = f'{originalName}.csv'         
            
          df.to_csv(csvName, index=False)          
          with open(csvName, mode='r') as movement_file:
            reader = csv.reader(movement_file)
            for row in reader:                
              data.append(row)
          #comment section to get the name of the property of the account 
          property = data[0][1].split(" ")[0]
          account_id = getBankIdFromFile("CAIXA", property) 
          #delete the first 3 rows (header)
          del data[0:3]
          
          #call the function to order the data 
          redDataCaixa(data, account_id)
          #delete the csv file after processing
          os.remove(csvName)
          #os.remove(path_file)
          
      elif(os.path.isfile(path_file) and path_file.endswith(('.csv', '.CSV'))):
        #process if the file have a checking or saving on the name
        if "checking" in file.lower()or 'saving' in file.lower():
          with open(path_file, mode='r') as movement_file:
              reader = csv.reader(movement_file)
              for row in reader:
                data.append(row)
              del data[0]
              
              account_id = getBankIdFromFile(file)              
              ReadCheckingOrSaving(data , account_id)
        
      
  except Exception as e:
    print("We have a error:")
    traceback.print_exc() 
check_folder(r"./files")
  
