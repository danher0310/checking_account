import os
import re
import csv
import pandas as pd
import traceback


def check_folder(path):
  try:
    files = os.listdir(path)
    for file in files:
      path_file = os.path.join(path, file)
      data =[]
      if os.path.isfile(path_file) and path_file.endswith(('.xls', '.XLS', 'XLSX', 'xlsx')):
        if "movimientos" in file.lower():
          df = pd.read_excel(path_file,engine='openpyxl')
          originalname = os.path.splitext(path_file)[0]
          
          csvName = f"{originalname}.csv"  
          df.to_csv(csvName, index=False)
          with open(csvName, mode='r') as movement_file:
            reader = csv.reader(movement_file)
            for row in reader:                
              data.append(row)
          del data[0:2]
          
          #print(data)
          os.remove(csvName)
          #os.remove(path_file)
      elif(os.path.isfile(path_file) and path_file.endswith(('.csv', '.CSV'))):
        print(file)
          
          
        
        
  except Exception as e:
    print("Se produjo un error:")
    traceback.print_exc() 
check_folder(r"./files")