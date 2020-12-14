// Databricks notebook source
// nasa july at FileStore/tables/nasa_19950701.tsv
// nasa august atFileStore/tables/nasa_19950801.tsv

// COMMAND ----------

val julyRDD =sc.textFile("/FileStore/tables/nasa_19950701.tsv")
val augustRDD =sc.textFile("/FileStore/tables/nasa_19950801.tsv")
val header1     =   julyRDD.first
val header2     =   augustRDD.first
val julyRdd = julyRDD.filter(line=> line !=header1)
val augustRdd = augustRDD.filter(line => line !=header2)



// COMMAND ----------

//Transformation ---narrow-  takes its own machine to process data 
//no data reshuffle
//trasferring data one place to another-wide
//union is wide transformation
val unionRDD = julyRdd.union(augustRdd)

// COMMAND ----------

// DBTITLE 1,INTERSECTION 
val intersectionRdd =  julyRdd.intersection(augustRdd)
intersectionRdd.map(line=>( line.split("\t")(0),line.split("\t")(1),line.split("\t")(2),line.split("\t")(3))).take(1)

// COMMAND ----------

//frist approact was using filter condition to not take header as data
//now 2nd approach

def headerRemover(line:String): Boolean = !(line.startsWith("host"))


// COMMAND ----------

unionRDD.filter(x=> headerRemover(x))

// COMMAND ----------

unionRdd.filter(x=>x.split("\t")(5).toInt==200).take(2)

// COMMAND ----------



// COMMAND ----------

val finalRDD = unionRdd.filter(x=> headerRemover(x))
finalRDD.take(2)

// COMMAND ----------

finalRDD.sample(withReplacement = true, fraction = .2).count()

// COMMAND ----------



// COMMAND ----------

// DBTITLE 1,Intersection on Host
val julyRDD =sc.textFile("/FileStore/tables/nasa_19950701.tsv")
val augustRDD =sc.textFile("/FileStore/tables/nasa_19950801.tsv")

val hostJuly = julyRDD.map(x=>x.split("\t")(0))
hostJuly.take(2)
val hostAugust = augustRDD.map(x=>x.split("\t")(0))
hostAugust.take(2)
val Hostintersection =  hostJuly.intersection(hostAugust)

// COMMAND ----------

// DBTITLE 1,Intersection on Host Without Header
val finalHostIntersection = Hostintersection.filter(x=> headerRemover(x))
finalHostIntersection.count()

// COMMAND ----------

// DBTITLE 1,KeyValue_data
//Action  = oprtation to remove the laziness of the RDD
//COLLECT
//TAKE --show biased output --not in a fixed order
//COUNT
//spark context- used to communicate to the exuter program by driver program
//issue with collect--it load the whole data from executer program 
// to driver program thus if may give error if there is not enough memory


//NORMAL rdd - collect(),count, take
//action on key-value pair RDD Ex = countByValue, reduce, reduceByKey()

val data = List("tushar","goyal","tudhar","sir","goyal","regex")

val dataRDD = sc.parallelize(data)
dataRDD.count()

// COMMAND ----------

dataRDD.countByValue()

// COMMAND ----------

val data2 = List(1,2,3,4,5)
val data2RDD = sc.parallelize(data2)


// COMMAND ----------

//Reduce- take two elements
//return one according to the function we applied
val productRDD = data2RDD.reduce((x,y)=>x*y)
productRDD
