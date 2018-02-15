import psycopg2
import re

def stringManipulation(str): 
  if "CREATE USER" in str:
    stmt= str.replace("WITH SYSID","")  
    stmt=stmt.replace("PASSWORD","")
    nums = stmt.split(' ')
    nums[4]="'"+nums[2].title()+'123'+"'"
    nums[3]="PASSWORD"
    stmt = ""
    for num in nums:
      if len(num) != 37: 
        stmt = stmt +" " +num
      
    return stmt
    
  elif "CREATE GROUP" in str:
      str=re.sub("WITH SYSID\s\d+;","",str)
      #str=re.sub(" ;",";",str)
     #str=""
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
#print(replace)

conn=psycopg2.connect(dbname= 'mydbrelus', host='harsharelus.cbqa1jhivb7b.us-east-2.redshift.amazonaws.com', 
port= '5439', user= 'harshatj', password= 'Tjswag37!')


cur = conn.cursor()
for i in replace:
   #print(i)
  
  try: 
     cur.execute(i)
     conn.commit()
     rs = cur.fetchall()
     conn.close()
     print(rs)
  except:
      print("Record Inserted")
