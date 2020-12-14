// Databricks notebook source
// DBTITLE 1,Loading Data
///FileStore/tables/numberData.csv

val dataRDD =sc.textFile("/FileStore/tables/numberData.csv")

// COMMAND ----------

// DBTITLE 1,Removing Header
val header =   dataRDD.first
val data2  =   dataRDD.filter(line=> line !=header)
data2.collect()

// COMMAND ----------

// DBTITLE 1,Function to check for PRIME number
def checkForPrime(x: Int): Boolean =
    if (x <= 1)
        false
    else if (x == 2)
        true
    else
        !(2 until x).exists(n => x % n == 0)

// COMMAND ----------

val prime = data2.filter(line=> checkForPrime(line.toInt))

// COMMAND ----------

val primeInt = prime.map(x=>x.toInt)
val sumOfPrime = primeInt.reduce((x,y)=>x+y)
sumOfPrime

// COMMAND ----------


