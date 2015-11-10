import org.apache.log4j.{Level, LogManager}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LassoWithSGD, LinearRegressionWithSGD, LinearRegressionModel, LabeledPoint}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by fzschornack on 02/11/15.
 */
object HowMuchDidItRain {

  def main(args: Array[String]) {

    val file = "example.csv"
    val trainFile = "train.csv"
    //val file ="example_test.csv"
    val testFile = "test.csv"

    val conf = new SparkConf().setAppName("How Much Did It Rain?").setMaster("local")
    val sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.WARN)

    //filterTrainData(trainFile, sc)

    //filterTestData(testFile, sc)

    //analyzeTrainModel(sc)

    //trainModel(sc)

    //testPrediction("rainLinearModel", sc)

    filterReflectivityMean(trainFile, sc)

  }


  def filterTrainData(file: String, sc: SparkContext): Unit = {
    val data = sc.textFile(file).zipWithIndex().filter(_._2 > 0)

    val filteredData = data.map { line =>
      val parts = line._1.split(",",-1).map(x =>
        x.equals("") match {
          case true => 0
          case false => x.toDouble
        })
      parts
    }
      .filter(x => x(3) != 0 && x.last < 70)
      .map(x => x.head -> x.tail.drop(1)) // drop Minutes
      .reduceByKey( (x, y) => (x, y).zipped.map(_ + _).dropRight(1) ++ Array(x(21)) ) // sum features with same Id
      .map(_._2) // drop Id

    filteredData.coalesce(1).map(x => x.mkString(",")).saveAsTextFile("filteredTrainData")
  }


  def trainModel(sc: SparkContext): Unit = {

    val filteredDataFile = sc.textFile("filteredTrainData").map(line => line.split(",").map(_.toDouble))

    val parsedData = filteredDataFile.map { parts =>

      // last = label, drop label
      LabeledPoint(parts.last, Vectors.dense(parts.tail.dropRight(1)))

    }.cache()

    //Building the model
    val numIterations = 100
    val rainLinearModel = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize = 1.0/50000)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = rainLinearModel.predict(point.features)
      (point.label, prediction)
    }

    val MAE = valuesAndPreds.map{case(v, p) => math.abs(v - p)}.mean()
    println("training Mean Absolute Error = " + MAE)

    // Save and load model
    rainLinearModel.save(sc, "rainLinearModel")
  }


  def analyzeTrainModel(sc: SparkContext): Unit = {

    val filteredDataFile = sc.textFile("filteredTrainData").map(line => line.split(",").map(_.toDouble))

    val parsedData = filteredDataFile.map { parts =>

      // last = label, drop label
      LabeledPoint(parts.last, Vectors.dense(parts.tail.dropRight(1)))

    }.cache()

    val splits = parsedData.randomSplit(Array(0.7, 0.3), seed = 10L)

    val training = splits(0).persist(StorageLevel.MEMORY_AND_DISK)

    val test = splits(1)

    //Building the model
    val numIterations = 100
    val rainLinearModel = LinearRegressionWithSGD.train(training, numIterations, stepSize = 1.0/50000)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = test.map { point =>
      val prediction = rainLinearModel.predict(point.features)
      (point.label, prediction)
    }

    valuesAndPreds.take(100).foreach(println)

    val MAE = valuesAndPreds.map{case(v, p) => math.abs(v - p)}.mean()
    println("training Mean Absolute Error = " + MAE)

  }


  def filterTestData(testFile: String, sc: SparkContext): Unit = {

    val data = sc.textFile(testFile).zipWithIndex().filter(_._2 > 0)

    val filteredData = data.map { line =>
      val parts = line._1.split(",",-1)
        .map(x =>
        x.equals("") match {
          case true => 0.0
          case false => x.toDouble
        })
      parts
    }
      .map(x => x.head -> x.tail.drop(1)) // drop Minutes
      .reduceByKey( (x, y) => (x, y).zipped.map(_ + _) ) // sum features with same Id

    filteredData.coalesce(1).map(x => x._1.toInt + "," + x._2.mkString(",")).saveAsTextFile("filteredTestData")

    filteredData.take(100).foreach(x => println(x._1.toInt + "," + x._2.mkString(",")))

  }


  def testPrediction(modelFile: String, sc: SparkContext): Unit = {

    val filteredDataFile = sc.textFile("filteredTestData").map(line => line.split(",").map(_.toDouble))

    val parsedData = filteredDataFile.map { parts =>

      (parts.head, Vectors.dense(parts.tail.drop(1)))

    }.cache()

    val model = LinearRegressionModel.load(sc, modelFile)

    val valuesAndPreds = parsedData.map { point =>
      val prediction = model.predict(point._2)
      (point._1.toInt, prediction)
    }

    valuesAndPreds.sortByKey().coalesce(1, true).map(x => x._1 + "," + x._2).saveAsTextFile(modelFile + "Solution")

  }

  def filterReflectivityMean(file: String, sc: SparkContext): Unit = {
    val data = sc.textFile(file).zipWithIndex().filter(_._2 > 0)

    val filteredData = data.map { line =>
      val parts = line._1.split(",",-1).map(x =>
        x.equals("") match {
          case true => 0
          case false => x.toDouble
        })
      Array(parts(0),parts(3),parts.last)
    }
      .filter(x => x(1) != 0 && x.last < 70)
      .map(x => x.head -> x.tail)

    val countById = filteredData.countByKey()

    val refMean = filteredData
      .reduceByKey( (x, y) => Array(x(0) + y(0)) ++ Array(x(1)) ) // sum features with same Id
      .map( x => x._1.toInt -> Array(x._2(0)/countById(x._1), x._2(1)))


    refMean.coalesce(1).sortByKey().map(x => x._1 + "," + x._2.mkString(",")).saveAsTextFile("filteredRefMeanData")
  }

}
