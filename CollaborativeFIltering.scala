import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating
val ratings_data = sc.textFile("ratings.dat")
val r = ratings_data.map { line =>
  val fields = line.split("::")
  (Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble))
}

val splitted = r.randomSplit(Array(0.6, 0.4), seed = 11L)
val train = splitted(0)
val test = splitted(1)
val rank = 10
val numIterations = 10
val model = ALS.train(train, rank, numIterations, 0.01)
val usersProducts = test.map { case Rating(user, product, rate) =>
  (user, product)
}

val predictions =
  model.predict(usersProducts).map { case Rating(user, product, rate) =>
    ((user, product), rate)
  }

val ratesAndPreds = test.map { case Rating(user, product, rate) =>
  ((user, product), rate)
}.join(predictions)

val MSE = ratesAndPreds.map { case ((user, product), (r1, r2)) =>
  val err = (r1 - r2)
  err * err
}.mean()

println("Accuracy = " + MSE)
