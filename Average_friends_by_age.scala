// Databricks notebook source
//FileStore/tables/FriendsData.csv
//max no of friends age wise

val data = sc.textFile("/FileStore/tables/FriendsData.csv")
val header =   data.first
val friendsRDD  =   data.filter(line=> line !=header)

// COMMAND ----------

val fRDD = friendsRDD.map(x=> (x.split(",")(2).toInt,(1, x.split(",")(3).toInt)) )
val reduceRdd = fRDD.reduceByKey((x,y)=> (x._1+y._1,x._2+y._2))
val finalRdd  = reduceRdd.mapValues(x=> (x._2 / x._1).toInt)
finalRdd.collect()
for((age,avgFrien)<-finalRdd.collect()) println(age+" : "+avgFrien)

// COMMAND ----------

// DBTITLE 1,age wise Max friends
val fRDD2 = friendsRDD.map(x=> (x.split(",")(2).toInt,( x.split(",")(3).toInt)) )
val finalR = fRDD2.reduceByKey((x,y)=> if(x>=y) x else y)
finalR.collect()
for((age,maxFrien)<-finalR.collect()) println(age+" : "+maxFrien)

// COMMAND ----------


