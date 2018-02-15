import psycopg2
import re

def stringManipulation(str): 
  if "CREATE USER" in str:
    stmt= str.replace("WITH SYSID","")  
    nums = stmt.split(' ')
    nums[6]="'"+nums[2].title()+'123'+"'"
    stmt = ""
    for num in nums:
        
      # Pick the username (num2) and append 123
      if len(num) != 3:   # Check any Python fucntion to check whether it is number or not
        
        stmt = stmt +" " +num
    return stmt
    
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
  
Read = open('C:\\Users\\Administrator\\Desktop\\assignment_redshift\\new.txt')

replace = list(map(stringManipulation,Read))


conn=psycopg2.connect(dbname= 'dbname', host='host', 
port= '5439', user= 'user', password= 'pwd!')


cur = conn.cursor()
for i in replace:
   print(i)
   try: 
      cur.execute(i)
   except:
       print(Exception)
    
conn.commit()

print(replace[0])