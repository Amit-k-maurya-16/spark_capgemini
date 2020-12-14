// Databricks notebook source
val AirportRdd = sc.textFile("/FileStore/tables/airports.text")
AirportRdd.take(1)

// COMMAND ----------

//Tuple <= dictionary

val dataRdd = AirportRdd.map(line=>( line.split(",")(0), line.split(",")(1)))
dataRdd.take(3)

// COMMAND ----------

//tuple demo
val tuple = ("Amit",2020)
tuple._2

// COMMAND ----------

// DBTITLE 1,Airports which are not in canada
val other_than_canada =dataRdd.filter(x=> x._2 != "\"Canada\"" )
 other_than_canada.take(1)

// COMMAND ----------

other_than_canada.filter(x => x._2 == "\"Canada\"").collect


// COMMAND ----------

val listdata = List("Tushar 2020","Nancy 1998","Abhinav 1997")

// COMMAND ----------

val listrdd = sc.parallelize(listdata)
val key_val = listrdd.map(x=> (x.split(" ")(0), x.split(" ")(1)))

// COMMAND ----------

key_val.mapValues(x=>x+10)

// COMMAND ----------

// DBTITLE 1,Key Value Pair of AirportName-Timestamp(in lower case)
val airport_times = AirportRdd.map(line=>( line.split(",")(1), line.split(",")(11)))
airport_times.mapValues(x=>x.toLowerCase()).take(1)

// COMMAND ----------

//reduce is a action
//reduceByKey is a transformation


 /////FileStore/tables/Property_data.csv
val propertyData = sc.textFile("/FileStore/tables/Property_data.csv")
val header =   propertyData.first
val propertyRDD  =   propertyData.filter(line=> line !=header)

// COMMAND ----------



// COMMAND ----------

val roomRdd = propertyRDD.map(x=> (x.split(",")(3).toInt,(1, x.split(",")(2).toDouble)) )


// COMMAND ----------

val reduceRdd = roomRdd.reduceByKey((x,y)=> (x._1+y._1,x._2+y._2))

// COMMAND ----------

val finalRdd  = reduceRdd.mapValues(x=> x._2 / x._1)

for((bedRoom,avg)<-finalRdd.collect()) println(bedRoom+" : "+avg)

// COMMAND ----------

finalRdd.saveAsTextFile("PropertyFinal.csv")

// COMMAND ----------


