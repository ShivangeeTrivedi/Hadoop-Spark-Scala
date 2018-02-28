val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val business = sc.textFile("business.csv").cache().map(x => x.split("::")).map(x => (x(0),x(1),x(2))).toDF("bis_id","address","categories")

val review = sc.textFile("review.csv").cache().map(x => x.split("::")).map(x => (x(1), x(2))).toDF("user_id", "bis_id")

review.registerTempTable("r")
val review_counts = sqlContext.sql("select bis_id,count(*) from r group by bis_id")

val rcDF = review_counts.toDF("bis_id","count")

business.createOrReplaceTempView("b")
rcDF.createOrReplaceTempView("rcDF")

val joinedTable = sqlContext.sql("SELECT b.bis_id, b.address,b.categories,rcDF.count FROM rcDF JOIN b ON rcDF.bis_id = b.bis_id group by b.bis_id, b.address,b.categories,rcDF.count order by rcDF.count desc limit 10").rdd.cache()
val output = joinedTable.map(x => (x(0),x(1),x(2),x(3)))
val output_cleaned = output.map(x => x._1.toString.replace("(","").replace(")","")+"\t"+x._2+"\t"+x._3+"\t"+x._4)

output_cleaned.repartition(1).saveAsTextFile("Question3_SparkSQL")

