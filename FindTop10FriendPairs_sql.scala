val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val socfile = sc.textFile("soc-LiveJournal1Adj.txt").cache()
val userdata = sc.textFile("userdata.txt").cache()
var user_friends = socfile.filter(x => (x.split("\t").size > 1)).map(x => x.split("\t")).map(x => (x(0),x(1).split(",").filter(!_.isEmpty).toList))

var userdata_cleaned = userdata.map(x  => x.split(",")).map(x => (x(0), x(1), x(2), x(3))).toDF("col1", "col2", "col3", "col4")
userdata_cleaned.registerTempTable("userdata_cleaned")

def paired(line: String): Array[(String, String)] = {
      val splitted = line.split("\t")
      val first = splitted(0)
      if (splitted.size > 1) {
        splitted(1).split(",").map {
          segment => {
            if (segment.toLong < first.toLong) {
              (segment, first)
            } else {
              (first, segment)
            }
          }
        }
      } else {
        Array(null)
      }
    }

val rdd = socfile.flatMap { line => paired(line) }.distinct()

val rdd1 = rdd.filter(x => x != null)
val rdd1_reversed = rdd1.map(x => (x._2, x._1))

val rdd1DF = rdd1.toDF("a","b")
val user_friendsDF = user_friends.toDF("user","friends")

rdd1DF.registerTempTable("rdd1DF")
user_friendsDF.registerTempTable("user_friendsDF")
val table1 = sqlContext.sql("select a,b,friends from rdd1DF join user_friendsDF on rdd1DF.a = user_friendsDF.user")

val table = sqlContext.sql("select a,b,friends from rdd1DF join user_friendsDF on rdd1DF.b = user_friendsDF.user")

val df1 = table1.toDF("a","b","f1")
val df2 = table.toDF("a","b","f2")

df1.registerTempTable("df1")
df2.registerTempTable("df2")

val inter = sqlContext.sql("select df1.a,df1.b,f1,f2 from df1 join df2 on df1.a = df2.a and df1.b = df2.b")

val interDF = inter.toDF("a","b","f1","f2")
interDF.registerTempTable("interDF")

spark.udf.register("array_intersect", (xs: Seq[String], ys: Seq[String]) => xs.intersect(ys).size)

val mutual = sqlContext.sql("select a,b,array_intersect(f1,f2) as count from interDF order by count desc limit 10")

val mutualDF = mutual.toDF("a","b","count")
mutualDF.registerTempTable("mutualDF")

val output = sqlContext.sql("select mutualDF.a, mutualDF.b, mutualDF.count, userdata_cleaned.col2, userdata_cleaned.col3, userdata_cleaned.col4 from mutualDF JOIN userdata_cleaned ON mutualDF.a = userdata_cleaned.col1").toDF("user1","user2","count","fn","ln","address")
output.registerTempTable("output")
val output1 = sqlContext.sql("select output.count, output.fn, output.ln, output.address, userdata_cleaned.col2, userdata_cleaned.col3, userdata_cleaned.col4 from output JOIN userdata_cleaned ON output.user2 = userdata_cleaned.col1").rdd.cache()
val output1_mapped = output1.map(x => (x(0), x(1), x(2), x(3), x(4), x(5), x(6)))
val output_cleaned = output1_mapped.map(x => (x._1.toString.replace(",","")+"\t"+x._2+"\t"+x._3+"\t"+x._4+"\t"+x._5+"\t"+x._6+"" +"\t"+x._7.toString.replace("[","").replace("]","")))
output_cleaned.collect().foreach(println)
output_cleaned.repartition(1).saveAsTextFile("Question2_SparkSQL")

