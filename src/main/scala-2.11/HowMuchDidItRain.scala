import org.apache.log4j.{Level, LogManager}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LassoWithSGD, LinearRegressionWithSGD, LinearRegressionModel, LabeledPoint}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by fzschornack on 02/11/15.
 */
object HowMuchDidItRain {

  def main(args: Array[String]) {

    //val file = "test.txt"
    val file = "/Users/fzschornack/kaggle/train.csv"

    val conf = new SparkConf().setAppName("How Much Did It Rain?").setMaster("local")
    val sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.WARN)

    val data = sc.textFile(file).zipWithIndex().filter(_._2 > 0)

    val parsedData = data.map { line =>
      val parts = line._1.split(",").map( x =>
        x.equals("") match {
          case true => "0"
          case false => x
        } )

      val indices = Array(1,2,3,7,11,15,19)
      LabeledPoint(parts.last.toDouble, Vectors.dense(indices.map(parts).map(_.toDouble)))
    }.cache()

    //parsedData.foreach(println)

//    val splits = parsedData.randomSplit(Array(0.7, 0.3), seed = 1L)
//
//    val training = splits(0).persist(StorageLevel.MEMORY_AND_DISK)
//
//    val test = splits(1)

    // Building the model
    val numIterations = 200
    val model = LassoWithSGD.train(parsedData, numIterations, 0.2, 1)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point.features)
      (point.label, prediction)
    }

    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
    println("training Mean Squared Error = " + MSE)

    // Save and load model
    model.save(sc, "rainModel")


//    val sameModel = LinearRegressionModel.load(sc, "rainModel")
//
//    val valuesAndPreds = parsedData.map { point =>
//      val prediction = sameModel.predict(point.features)
//      (point.label, prediction)
//    }
//
//    valuesAndPreds.take(100).foreach(println)
//
//    val MSE = valuesAndPreds.map{case(v, p) => math.pow((v - p), 2)}.mean()
//    println("training Mean Squared Error = " + MSE)


  }

}
