val socfile = spark.sparkContext.textFile("soc-LiveJournal1Adj.txt").cache()
val userdata = spark.sparkContext.textFile("userdata.txt").cache()

var user_friends = socfile.filter(line => (line.split("\t").size > 1)).map(line => line.split("\t")).map(tokens => (tokens(0),tokens(1).split(",").filter(!_.isEmpty).toList))

val userdata_cleaned = userdata.map(line => line.split(",")).map(x => (x(0),x(1),x(2),x(3)))

    def createPair(line: String): Array[(String, String)] = {
      val splits = line.split("\t")
      val kuid = splits(0)
      if (splits.size > 1) {
        splits(1).split(",").map {
          segment => {
            if (segment < kuid) {
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

val rdd = socfile.flatMap { line => createPair(line) }.distinct()

val rdd1 = rdd.filter(x => x != null)
val reversedrdd1 = rdd1.map(x => (x._2, x._1))

val userfriends_mapped = user_friends.map(x => (x._1, x))
val rdd1friends = rdd1.join(userfriends_mapped).cache()
val rdd2friends = reversedrdd1.join(userfriends_mapped).cache()

val rdd1friends_mapped = rdd1friends.map(x => (x._1, x._2._1, x._2._2._2))
val rdd2friends_mapped = rdd2friends.map(x => (x._2._1, x._1, x._2._2._2))

val friends = rdd1friends_mapped.map {case (key1, key2, value) => ((key1, key2), value)}.join(rdd2friends_mapped.map {case (key1, key2, value) => ((key1, key2), value)})

val inter = friends.map(x => (x._1,x._2._1.intersect(x._2._2).size))
val inter_sorted = inter.sortBy(_._2,false)

val inter_sortedmapped =inter_sorted.map(x => (x._1._1,x._1._2,x._2))

val inter_sorted1 = inter_sorted.map(x => (x._1._1,x._1._2))
val inter_sorted2 = inter_sorted.map(x => (x._1._2,x._1._1))

val user_mapped = userdata_cleaned.map(x => (x._1,x))
val joined = inter_sorted1.join(user_mapped)

val jstring = joined.map(x => (x._1, x._2._1, (x._2._2._2,x._2._2._3,x._2._2._4).toString()))

val userjoininter = inter_sorted2.join(user_mapped)
val ustring = userjoininter.map(x => (x._2._1, x._1, (x._2._2._2,x._2._2._3,x._2._2._4).toString()))

val users_joined = jstring.map {case (key1, key2, value) => ((key1, key2), value)}.join(ustring.map { case (key1, key2, value) => ((key1, key2), value)})

val a = users_joined.map(x => (x._1._1,x._1._2,x))
val b = inter_sortedmapped.map { case (key1, key2, value) => ((key1, key2), value)}.join(a.map {case (key1, key2, value) => ((key1, key2), value)})

val output = b.map(x => (x._1._1,x._1._2,x._2._1,x._2._2._2._1,x._2._2._2._2))

val outputsorted = output.sortBy(_._3,false).zipWithIndex.filter{case (_, count) => count <= 10}.keys

val output_cleaned = outputsorted.map(x=> (x._3.toString + "\t" + x._4.toString().replace(",","\t").replace("(","").replace(")","") + "\t" + x._5.toString().replace(",","\t\t\t").replace("(","").replace(")","")))

output_cleaned.repartition(1).saveAsTextFile("Question2")
