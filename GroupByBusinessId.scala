val business = sc.textFile("business.csv").cache().map(x => x.split("::")).map(x => (x(0),x))

val review = sc.textFile("review.csv").cache().map(x => x.split("::")).map(x => (x(2)))

val top10 = review.map(x => (x,1)).reduceByKey(_+_).map(x => (x._1,x._2)).sortBy(_._2,false).take(10)

val parallelized = sc.parallelize(top10)

val joined = parallelized.distinct().join(business).values
val output = joined.map(x => (x._2.mkString("\t"),x._1)).distinct().sortByKey(false)
val output_cleaned = output.map(x => (x._1+"\t"+x._2))
output_cleaned.repartition(1).saveAsTextFile("Question3")



