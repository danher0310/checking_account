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
  date_obj =datetime.strptime(date, '%m/%d/%Y').date()
  return date_obj 

def converDateToWise(date):
  date = date.split(" ")[0]
  date_obj = datetime.strptime(date, '%Y-%m-%d').date()
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
def checkMovementDb(account_id): 
  try: 
     #check the transaction and the times of the repeat of the transaction on the database.
    mydb = connectionDb()
    myCursor = mydb.cursor()      
    query = '''
            SELECT payment_date, payment_description, payment_amount FROM movement WHERE card_id = %s; 
        '''
    myCursor.execute(query, (account_id, ))
    result = myCursor.fetchall()
    
    myCursor.close()  
    return result
  except:
    return('An error occurred checking movement of the cards')
  
  
#funtion to order all transactions before checking the database and compare  
def processTransaction(data, account_id):
  
  
  try:    
    #Get the transaction from the database and convert the result to array 
    data_from_DB = checkMovementDb(account_id)
    data_from_DB_normalized =[
      [row[0], row[1], float(row[2])] for row in data_from_DB
    ]  
    #compare the data from the file and the database
    datatoDB = [
      [account_id] + row 
      for row in data 
      if row not in data_from_DB_normalized
    ]
    #register the transactions on the database if the transactions is not in the database
    registerTransaction(datatoDB)  
    
    
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
      amount = float(row[3])
      parser_data.append([date, description, amount])
    
    
    processTransaction(parser_data, account_id)
      
   
  except Exception as e:
    print("An error occurred:")
    traceback.print_exc() 

def readWise(data, account_id):
  try:
    parser_data = []
    #erxtract date, description, and amount from de data of csv file
    
    for row in data:  
      
      date = converDateToWise(row[3])       
      if row[16] !="":    
        description = row[16]
      else:
        description = row[12]
      amount = float(row[13])      
      if row[2].lower() == 'in':        
        amount = float(row[13])
      elif row[2].lower() == 'out':
        amount = float(row[13])* -1
      
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
          if account_id != False:
            redDataCaixa(data, account_id)
            #delete the csv file after processing
            os.remove(csvName)
            os.remove(path_file)
          else:
            continue
          
      elif(os.path.isfile(path_file) and path_file.endswith(('.csv', '.CSV'))):
        #process if the file have a checking or saving on the name
        if "checking" in file.lower()or 'saving' in file.lower():
          with open(path_file, mode='r') as movement_file:
              reader = csv.reader(movement_file)
              for row in reader:
                data.append(row)
              del data[0]
          account_id = getBankIdFromFile(file)   
          if account_id != False:           
            ReadCheckingOrSaving(data , account_id)
            os.remove(path_file)
          else:
            continue
          
        elif "wise" in file.lower():
          with open(path_file, mode='r') as movement_file:
              reader = csv.reader(movement_file)
              for row in reader:
                data.append(row)
              
              del data[0]
              file = file.split('-')[0]
          account_id = getBankIdFromFile(file)
          if account_id != False: 
            readWise(data, account_id)
            os.remove(path_file)
          else:
            continue
          
  except Exception as e:
    print("We have a error:")
    traceback.print_exc() 
check_folder(r"./files")
  
