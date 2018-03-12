package com.harshaassignment.pkgharsha

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import java.sql.{Date, Timestamp}

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.util
import java.util.Calendar

case class status(id:String,Name:String,Date:String,Status:String)

object assignment_harsha {
  
   package com.assignment.harsha_assignment

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.{Date, Timestamp}

import java.util
import java.util.Calendar

case class status(id:String,Name:String,Date:String,Status:String)

object sparksql_assignment {
  
  def main(Args:Array[String]){
   // System.setProperty("hadoop.home.dir","C:/Pyspark/HADOOP_2.7")
    
    val spark = new SparkSession.Builder()
                    .appName("dataframe")
                    .master("local")
                    .getOrCreate()
          
    import spark.implicits._
         
    //Read History file
    
    val df = spark.read.format("csv").option("header", true).option("inferschema", true)
            .load("D:/harshaAssignment/history.csv")
    df.registerTempTable("history")

    val dftype1 = spark.read.format("csv").option("header", true)
              .option("inferschema", true)
              .load("D:/harshaAssignment/type1.csv")
              
    //Read Type2 file
              
      val dftype2 = spark.read.format("csv").option("header", true)
              .option("inferschema", true)
              .load("D:/harshaAssignment/type2.csv")        
    
  

    //Logic for getting yesterday date
    val cal = java.util.Calendar.getInstance
    
    // Get today as a Calendar
    val today = java.util.Calendar.getInstance();
    // Subtract 1 day
    today.add(Calendar.DATE, -1);
    // Make an SQL Date out of that
    val yesterday = new java.sql.Date(today.getTimeInMillis());
    //println("Yesterday= " + yesterday)
    
    //Retrieve all the records for yesterdy
    val dfhistory = spark.sql("select id,Name, Date_time,mobile from history where Date_time='" + yesterday.toString()  + "'" )
      
    
    //Join history and Type1
      
    val dfhisttype1 = dftype1.join(dfhistory,dfhistory("id")===dftype1("id") , "left_outer")
    
    println("DfType1")
    dftype1.show()
    
    println("History")
    dfhistory.show()
    
    println("Join")
    dfhisttype1.show()
    
    println("Printing Status for Type1 file")

    
    val rddstatustype1 = dfhisttype1.rdd
                            .map(f => 
                              if (f(5)==null) f(0) + "," + f(1) + "," + f(3) + ","  + "Insert" 
                              else if(f(4) != f(8)) f(0) + "," + f(1) + "," + f(3) + ","  + "Update" 
                              else f(0) + "," + f(1) + "," + f(3) + ","  + " " )
    
    val rddwithschema1 = rddstatustype1.map(f => f.split(","))
                   .map(f => status(f(0),f(1),f(2),f(3)))
                   
    val dfStatus1 = rddwithschema1.toDF()
    
    dfStatus1.show()
    
    
    //Logic for Type2 file
    
    val dfhisttype2 = dftype2.join(dfhistory,dfhistory("id")===dftype2("id") , "left_outer")
    
    
    
    println("DfType2")
    dftype2.show()
    
    
    println("Printing Status for Type2 file")

    
    val rddstatustype2 = dfhisttype2.rdd
                            .map(f => 
                              if (f(5)==null) f(0) + "," + f(1) + "," + f(3) + ","  + "Insert" 
                              else if(f(4) != f(8)) f(0) + "," + f(1) + "," + f(3) + ","  + "Update" 
                              else f(0) + "," + f(1) + "," + f(3) + ","  + " " )
    
    val rddwithschema2 = rddstatustype2.map(f => f.split(","))
                   .map(f => status(f(0),f(1),f(2),f(3)))
                   
    val dfStatus2 = rddwithschema2.toDF()
    
    dfStatus2.show()
   }
  
}
  
}