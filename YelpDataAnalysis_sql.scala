val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val business = sc.textFile("business.csv").cache().map(line => line.split("::")).map(line => (line(0),line(1))).toDF("col1","col2")
business.registerTempTable("business")
val businessDF = sqlContext.sql("select distinct col1 from business where col2 like '%Palo Alto%'")
businessDF.createOrReplaceTempView("b")

val reviewDF = sc.textFile("/home/shivangee/Documents/BIg Data/Homework-2/review.csv").cache().map(x => x.split("::")).map(x => (x(0), x(1), x(2), x(3))).toDF("col1", "col2", "col3", "col4")

reviewDF.createOrReplaceTempView("r")
val output1 = sqlContext.sql("SELECT r.col2, r.col4 FROM r JOIN b ON b.col1 = r.col3").rdd.cache()

val output = output1.map(x => (x(0),x(1)))
val output_cleaned = output.map(x => (x._1.toString.replace("[","").replace("]","")+"\t"+x._2.toString.replace("]","").replace("[","")))

output_cleaned.repartition(1).saveAsTextFile("Question4_SparkSQL")

