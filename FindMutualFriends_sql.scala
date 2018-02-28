val sqlContext = new org.apache.spark.sql.SQLContext(sc)

val socfile = sc.textFile("soc-LiveJournal1Adj.txt").cache()

var user_friends = socfile.filter(x => (x.split("\t").size > 1)).map(x => x.split("\t")).map(x => (x(0),x(1).split(",").filter(!_.isEmpty).toList))


    def paired(line: String): Array[(String, String)] = {
      val splits = line.split("\t")
      val kuid = splits(0)
      if (splits.size > 1) {
        splits(1).split(",").map {
          segment => {
            if (segment.toLong < kuid.toLong) {
              (segment, kuid)
            } else {
              (kuid, segment)
            }
          }
        }
      } else {
        Array(null)
      }
    }

    val rdd = socfile.flatMap { line => paired(line) }.distinct()

    val rdd1 = rdd.filter(x => x != null)
    val reversed_rdd1 = rdd1.map(x => (x._2, x._1))

    val rdd1DF = rdd1.toDF("a","b")
    val user_friendsDF = user_friends.toDF("user","friends")

    rdd1DF.registerTempTable("rdd1DF")
    user_friendsDF.registerTempTable("user_friendsDF")
    val table1 = sqlContext.sql("select a, b, friends from rdd1DF join user_friendsDF on user_friendsDF.user = rdd1DF.a")

    val table2 = sqlContext.sql("select a, b, friends from rdd1DF join user_friendsDF on user_friendsDF.user = rdd1DF.b")

    val df1 = table1.toDF("a","b","f1")
    val df2 = table2.toDF("a","b","f2")

    df1.registerTempTable("df1")
    df2.registerTempTable("df2")

    val mutual_friends = sqlContext.sql("select df1.a,df1.b,f1,f2 from df1 join df2 on df1.a = df2.a and df1.b = df2.b")

    mutual_friends.take(10).foreach(println)
    val mutual_friendsDF = mutual_friends.toDF("a","b","afriends","bfriends")
    mutual_friendsDF.registerTempTable("mutual_friendsDF")

    spark.udf.register("array_intersect",
      (xs: Seq[String], ys: Seq[String]) => xs.intersect(ys).size)

    val output1 = sqlContext.sql("select a, b, array_intersect(afriends,bfriends) from mutual_friendsDF")

    val output = output1.rdd.map(x => (x(0),x(1),x(2)))
    val output_cleaned = output.map(x => x._1.toString().replace("[","")+","+x._2.toString.replace("]","")+"\t"+x._3)

    output_cleaned.repartition(1).saveAsTextFile("Question1_SparkSQL")
    
