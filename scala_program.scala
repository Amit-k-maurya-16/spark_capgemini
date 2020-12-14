// Databricks notebook source
var a=1

a


// COMMAND ----------

lazy val c =5
val new1 = List(1,2,3,4)

// COMMAND ----------

new1.head
val new2 = Array(2,3,4,5)


// COMMAND ----------

//parallelize method to create RDD and it is lazy in nature

val creationRDD = sc.parallelize(new1)
creationRDD.collect()

// COMMAND ----------

creationRDD.partitions.size
val rddPartition = sc.parallelize(new1,2)
rddPartition.partitions.size
rddPartition.count()

// COMMAND ----------

//map to create to create new RDD from existing rdd
rddPartition.map(c=>c*c*c).collect()

// COMMAND ----------

rddPartition.filter(x=>x%2==0).collect

// COMMAND ----------

val keyrdd = rddPartition.map(x=>(x,1))
keyrdd.collect

// COMMAND ----------

keyrdd.reduceByKey(_+_).collect()

// COMMAND ----------


