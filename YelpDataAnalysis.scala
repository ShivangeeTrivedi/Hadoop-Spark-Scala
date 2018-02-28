val business = sc.textFile("/home/shivangee/Documents/BIg Data/Homework-2/business.csv").cache()
val review = sc.textFile("/home/shivangee/Documents/BIg Data/Homework-2/review.csv").cache()

val cleaned_bisnss = business.filter(line => line.contains("Palo Alto")).map(x => x.split("::")).map(x => (x(0), x(0)))

val cleaned_review = review.map(x => x.split("::")).map(x => (x(2), x))

val output = cleaned_bisnss.distinct().join(cleaned_review).values.values.map(x => (x(1), x(3)))
val output_cleaned = output.map(x => x._1.toString.replace("[","")+"\t"+x._2.toString.replace("]",""))

output_cleaned.repartition(1).saveAsTextFile("Question4")


