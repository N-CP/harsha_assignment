import psycopg2
import re

def stringManipulation(str): 
  if "CREATE USER" in str:
    nums = str.split(' ')
    str = ""
    for num in nums:
      # Pick the username (num2) and append 123
      if len(num) != 3:   # Check any Python fucntion to check whether it is number or not
        str = str + " "+ num + " "
    return str.replace("SYSID", "").replace("WITH", "")
  elif "CREATE GROUP" in str:
      str=re.sub("WITH SYSID\s\d+","",str)
      return str
      ''' nums = str.strip(';').split(' ')
       
       str = ""
       for num in nums:
     
         if len(num) != 3:   
           str = str + " "+ num + " "
           
       return str.replace("SYSID", "").replace("WITH", "")'''
   
  elif "ALTER GROUP" in str:
      return str
  elif "CREATE DATABASE" in str:
      return str
  elif "ALTER DATABASE" in str:
      return str
  elif "ALTER USER" in str:
      return str
  elif "SET" in str:
      return str    
  else: 
    return " " # Logic need to write, either empty string or word  
    
ReadRDD = open('C:\\Users\\Administrator\\Desktop\\nr.txt')

conn=psycopg2.connect(dbname= 'dbname', host='host', 
port= '5439', user= 'user', password= 'pwd')


cur = conn.cursor()


replaceRDD1 = list(map(stringManipulation,ReadRDD))

cur.execute(str(replaceRDD1))


# Take filter RDD and then write the Python code to push it to RS

print(replaceRDD1)
