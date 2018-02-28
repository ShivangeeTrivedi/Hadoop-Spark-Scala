import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

val item_user_mat = sc.textFile("itemusermat")
val parsedData = item_user_mat.map(s => Vectors.dense(s.split(" ").map(_.toDouble))).cache()
val numClusters = 10
val numIterations = 100
val clusters = KMeans.train(parsedData, numClusters, numIterations)
val pred = item_user_mat.map{x =>(x.split(" ")(0), clusters.predict(Vectors.dense(x.split(' ').map(_.toDouble))))}
val movies = sc.textFile("movies.dat")
val moviesParsed = movies.map(x => (x.split("::"))).map(x => (x(0), (x(1), x(2))))
val joined = pred.join(moviesParsed)
val output = joined.map(x => (x._2._1, (x._1, x._2._2)))
val output1  = output.groupByKey()
val final_output = output1.mapValues(iter => (iter.take(5)))
val cleaned_output = final_output.map(x => println("Cluster") + x._1.toString().replace("(","").replace(")","") + "\n" + x._2.toString().replace("(","").replace(")","").replace("List",""))
cleaned_output.repartition(1).saveAsTextFile("Question-1Output")
