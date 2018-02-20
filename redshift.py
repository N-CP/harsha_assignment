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
      #nums = str.split(' ')
      #str= str.replace("SYSID", "").replace("WITH", "")
      str=re.sub("WITH SYSID\s\d+;",";",str)
      str=re.sub(" ;",";",str)
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
  elif "CREATE TABLE" or ";" in str:
     
          return str
  elif "ALTER TABLE" in str:
      return str
  elif "CREATE SCHEMA" in str:
      return str    
  elif "ALTER SCHEMA" in str:
      return str  
  elif "ALTER INDEX" in str:
      return str  
  elif "CREATE VIEW" in str:
      return str 
  elif "CREATE FUNCTION" in str:
      str=str.replace(":","")
      str=str.replace("STRICT;",";")
      str=str.replace("IMMUTABLE","")
      str=str.replace("$_$","")
      str=str.replace("AS","STABLE AS $_$ select endtime")
      str=str.replace("LANGUAGE","$_$    LANGUAGE")
      return str 
  elif "ALTER FUNCTION" in str:
      return str
  else: 
    return " " # Logic need to write, either empty string or word 
   
  
inputread=open('C:\\Users\\admin\\Desktop\\syntex.txt',"r+") 
output=(str(list(map(stringManipulation,inputread))))
output1=re.sub('\[','',output)
output2=re.sub('\]','',output1)
output2=re.sub("\' '","",output2)
output2=re.sub('None','',output2)
output2=re.sub('\"',"",output2)
output2=re.sub('None','',output2)
output2=re.sub("\',",'',output2)
output2=re.sub("\, ' ","",output2)
output2=re.sub("\' ",'',output2)
print(output2)
with open('C:\\Users\\admin\\Desktop\\ab2.ddl',"w") as file:
   
   file.write(output2)
   
    
file.close()
