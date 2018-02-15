import psycopg2
import re

def stringManipulation(str): 
  if "CREATE USER" in str:
      
    nums = str.split(' ')
    nums[2]=nums[2].title()+'123'
    str = ""
    for num in nums:
        
      # Pick the username (num2) and append 123
      if len(num) != 3:   # Check any Python fucntion to check whether it is number or not
        
        str = str + " "+ num + " "
    return str.replace("SYSID", "").replace("WITH", "")
    
  elif "CREATE GROUP" in str:
      str=re.sub("WITH SYSID\s\d+","",str)
      return str
      
   
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
  
Read = open('C:\\Users\\admin\\Desktop\\nr.txt')

replace = list(map(stringManipulation,Read))

repstr=str(replace).strip('[]')
repstr=re.sub("\,","",repstr)
repstr=re.sub('\"',"",repstr)

# Take filter RDD and then write the Python code to push it to RS

conn=psycopg2.connect(dbname= 'dbname', host='host', 
port= '5439', user= 'user', password= 'pwd')


cur = conn.cursor()


replaceRDD1 = list(map(stringManipulation,ReadRDD))

cur.execute(str(repstr))


# Take filter RDD and then write the Python code to push it to RS

print(repstr)
