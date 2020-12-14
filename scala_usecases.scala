// Databricks notebook source
///FileStore/tables/ratings.csv
//creating rdd from exernal dataset

val data = sc.textFile("/FileStore/tables/ratings.csv")
data.collect()

// COMMAND ----------

val ratingsData = data.map(x=>x.split(",")(2))
ratingsData.countByValue()

// COMMAND ----------


//AIRPORT RDD  /FileStore/tables/airports.text


val AirportRdd = sc.textFile("/FileStore/tables/airports.text")
AirportRdd.collect()


// COMMAND ----------

val USAairport = AirportRdd.filter(x=>x.split(",")(3)=="\"United States\"")
USAairport.collect()


// COMMAND ----------

def splitInput(line:String) = {
 
  val dataSplit = line.split(",")
 
  val airportId = dataSplit(1)
 
  val cityName = dataSplit(2)
 
  (airportId, cityName)
}

// COMMAND ----------

USAairport.map(line => {
  val splitData = line.split(",")
  splitData(1)+","+splitData(2)
}).take(1)

// COMMAND ----------

////homework
//latitude >40 or name = iceland
//save into a file CountryIsland.csv


//Pacific/port->how many time && altitude is even
val task1 =AirportRdd.filter(x=>x.split(",")(7).toDouble>40 || x.split(",")(3)=="\"Iceland\"")
task1.saveAsTextFile("CountryIsland.csv")



// COMMAND ----------

//Pacific/port->how many time && altitude is even
val task2 = AirportRdd.filter(x=>x.split(",")(11)=="\"Pacific/Port_Moresby\"" && x.split(",")(8).toInt%2==0).collect



// COMMAND ----------


