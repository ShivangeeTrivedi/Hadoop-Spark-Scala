val socfile = sc.textFile("soc-LiveJournal1Adj.txt")
val userdata = sc.textFile("userdata.txt")

var user_friends = socfile.filter(x => (x.split("\t").size > 1)).map(x => x.split("\t")).map(x => (x(0),x(1).split(",").filter(!_.isEmpty).toList))


def paired(line: String): Array[(String, String)] = {
	val splitted = line.split("\t")
        val first = splitted(0)
        if(splitted.size > 1) {
          splitted(1).split(",").map {
            segment => {
              if(segment < first) {
                (segment, first)
              }else {
                (first,segment)
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

val uf_mapped = user_friends.map(x => (x._1, x))

val rdd1_joined = rdd1.join(uf_mapped).cache()
val rdd1_reversed_joined = rdd1_reversed.join(uf_mapped).cache()

val a = rdd1_joined.map(x => (x._1, x._2._1, x._2._2._2))
val b = rdd1_reversed_joined.map(x => (x._2._1, x._1, x._2._2._2))

      val output1 = a.map {
        case (key1, key2, value) => ((key1, key2), value)
      }.join(
          b.map {
            case (key1, key2, value) => ((key1, key2), value)
          })

val output = output1.map(x => (x._1,x._2._1.intersect(x._2._2).size))
val output_cleaned = output.map(x => x._1.toString().replace("(","").replace(")","")+"\t"+x._2)

output_cleaned.repartition(1).saveAsTextFile("Question-1")
