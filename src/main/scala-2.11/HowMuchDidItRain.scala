import breeze.numerics.log
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.mllib.clustering.{KMeansModel, KMeans}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.{LassoWithSGD, LinearRegressionWithSGD, LinearRegressionModel, LabeledPoint}
import org.apache.spark.mllib.stat.Statistics
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

    //runKmeansOnData(sc)

    //trainModel(sc)

    testPrediction("rainLinearModelFinal", sc)

    //filterReflectivityMean(testFile, sc)

    //testRefPrediction(sc)

    //filterReflectivityZdrKdpMean(trainFile, sc)

  }

  def runKmeansOnData(sc: SparkContext): Unit = {
    val filteredDataFile = sc.textFile("filteredTrainDataMeans").map(line => line.split(",").map(_.toDouble))

    val parsedData = filteredDataFile.map { parts =>

      // last = label, drop label
      Vectors.dense(parts.tail.dropRight(1))

    }.cache()

    //Building the model
    val numIterations = 200

    val numClusters = 4

    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("K = " + numClusters + "  Within Set Sum of Squared Errors = " + WSSSE)

    clusters.save(sc, "kMeansModel4")

  }

  /**
   * Version 2 - Id, Marshall_Palmer, # Zero_Refs, Features_Sum..., Label
   *
   * @param file
   * @param sc
   */
  def filterTrainData(file: String, sc: SparkContext): Unit = {
    val data = sc.textFile(file).zipWithIndex().filter(_._2 > 0)

    val filteredData = data.map { line =>
      val parts = line._1.split(",", -1).map(x =>
        x.equals("") match {
          case true => 0
          case false => x.toDouble
        })
      parts
    }
      .filter( x => x.last <= 120)
      .map(x => x.head.toInt -> x.tail.drop(1)) // drop Minutes

    val zeroCountById = filteredData.filter( x => x._2(1) == 0 ).countByKey()

    val countById = filteredData.countByKey()

    val filteredDataRef = filteredData.filter( x => x._2(1) > 0)

    val dataMeans = filteredDataRef
      .reduceByKey( (x, y) => (x, y).zipped.map(_ + _) )
      .map { x =>
      x._1 -> ( Array(-1) // bias
        ++ Array(math.exp( (math.log(math.abs(x._2(1))/countById(x._1)) - math.log(200))/1.6)) // marshall-palmer
        ++ Array(zeroCountById.contains(x._1) match { case true => zeroCountById(x._1) case false => 0} ) // zero counts
        ++ x._2.dropRight(4).map( _ / countById(x._1)) // features means
        ++ x._2.dropRight(4) // features sum
        ++ Array(x._2.last/countById(x._1)) // expected
        )
    }

    dataMeans.coalesce(1).map(x => x._1 + "," + x._2.mkString(",")).saveAsTextFile("trainFinal")
  }

  def filterTestData(testFile: String, sc: SparkContext): Unit = {

    val data = sc.textFile(testFile).zipWithIndex().filter(_._2 > 0)

    val filteredData = data.map { line =>
      val parts = line._1.split(",", -1).map(x =>
        x.equals("") match {
          case true => 0
          case false => x.toDouble
        })
      parts
    }
      .filter( x => x.last <= 120)
      .map(x => x.head.toInt -> x.tail.drop(1)) // drop Minutes

    val zeroCountById = filteredData.filter( x => x._2(1) == 0 ).countByKey()

    val countById = filteredData.countByKey()

    //val filteredDataRef = filteredData.filter( x => x._2(1) > 0)

    val dataMeans = filteredData
      .reduceByKey( (x, y) => (x, y).zipped.map(_ + _) )
      .map { x =>
      x._1 -> ( Array(-1) // bias
        ++ Array(math.exp( (math.log(math.abs(x._2(1))/countById(x._1)) - math.log(200))/1.6)) // marshall-palmer
        ++ Array(zeroCountById.contains(x._1) match { case true => zeroCountById(x._1) case false => 0} ) // zero counts
        ++ x._2.dropRight(3).map( _ / countById(x._1)) // features means
        ++ x._2.dropRight(3) // features sum
        //++ Array(x._2.last/countById(x._1)) // expected
        )
    }

    dataMeans.coalesce(1).map(x => x._1 + "," + x._2.mkString(",")).saveAsTextFile("testFinal")

  }


  def trainModel(sc: SparkContext): Unit = {

    val filteredDataFile = sc.textFile("trainFinal").map(line => line.split(",").map(_.toDouble))

    val parsedData = filteredDataFile.map { parts =>

      // last = label, drop label
      LabeledPoint(parts.last, Vectors.dense(parts.tail.dropRight(1)))

    }.persist(StorageLevel.MEMORY_AND_DISK)

    val elmParsedData = parsedData//ELM.transformation(parsedData, 50, 1)

    elmParsedData.take(100).foreach(println)



//    val elmParsedDataSplit = elmParsedData.randomSplit(Array(0.7, 0.3), 1)
//
//    val train = elmParsedDataSplit(0)
//
//    val test = elmParsedDataSplit(1)
//
//    train.persist(StorageLevel.MEMORY_AND_DISK)

    //Building the model
    val numIterations = 200
    val rainLinearModel = LinearRegressionWithSGD.train(parsedData, numIterations, stepSize = 1.0/48500)//, regParam = 2)

    println(rainLinearModel.weights)

    // Evaluate model on training examples and compute training error
    val valuesAndPreds = parsedData.map { point =>
      val prediction = math.max(0, rainLinearModel.predict(point.features))
      (point.label, prediction)
    }

    valuesAndPreds.take(200).foreach(println)

    val MAE = valuesAndPreds.map{case(v, p) => math.abs(v - p)}.mean()
    println("training Mean Absolute Error = " + MAE)

    // Save and load model
    rainLinearModel.save(sc, "rainLinearModelFinal")
  }


  def testPrediction(modelFile: String, sc: SparkContext): Unit = {

    val filteredDataFile = sc.textFile("testFinal").map(line => line.split(",").map(_.toDouble))

    val parsedData = filteredDataFile.map { parts =>

      LabeledPoint(parts.head, Vectors.dense(parts.tail))

    }.cache()

    parsedData.take(10).foreach(println)

    val model = LinearRegressionModel.load(sc, modelFile)

    val valuesAndPreds = parsedData.map { point =>
      val prediction = math.max(0, model.predict(point.features))
      (point.label, prediction)
    }

    valuesAndPreds.sortByKey().coalesce(1, true).map(x => x._1.toInt + "," + x._2).saveAsTextFile(modelFile + "SolutionFinal")

  }

  def testRefPrediction(sc: SparkContext): Unit = {

    val filteredDataFile = sc.textFile("filteredRefMeanTestData").map(line => line.split(",").map(_.toDouble))

    // log(R) = -3.0298 + 1.0523*log(Z)
    val valuesAndPreds = filteredDataFile.map( x => (x(0).toInt -> math.exp(-3.0298 + 1.0523*math.log(x(1))) ))

//    val mae = valuesAndPreds.map( x => math.abs( x._1 - x._2 ) ).mean()
//
//    println("training Mean Absolute Error = " + mae)
//
//    valuesAndPreds.take(1000).foreach(println)

    valuesAndPreds.sortByKey().coalesce(1, true).map(x => x._1 + "," + x._2).saveAsTextFile("SolutionAbsRef")

  }

  def filterReflectivityZdrKdpMean(file: String, sc: SparkContext): Unit = {
    val data = sc.textFile(file).zipWithIndex().filter(_._2 > 0)

    val filteredData = data.map { line =>
      val parts = line._1.split(",",-1).map(x =>
        x.equals("") match {
          case true => 0
          case false => x.toDouble
        })
      Array(parts(0),parts(3),parts(15),parts(19),parts.last)
    }
      .filter(x => x(1) > 0 && x.last < 70)
      .map(x => x.head -> x.tail)

    val countById = filteredData.countByKey()

    val refMean = filteredData
      .reduceByKey( (x, y) => Array(x(0) + y(0), x(1) + y(1), x(2) + y(2)) ++ Array(x.last) )
      .map( x => x._1.toInt -> Array(x._2(0)/countById(x._1), x._2(1)/countById(x._1), x._2(2)/countById(x._1), x._2(3)))


    refMean.coalesce(1).sortByKey().map(x => x._1 + "," + x._2.mkString(",")).saveAsTextFile("filteredRefZdrKdpMeanData")
  }

  def filterExpected(file: String, sc: SparkContext): Unit = {
    val data = sc.textFile(file).zipWithIndex().filter(_._2 > 0)

    val filteredData = data.map { line =>
      val parts = line._1.split(",",-1).map(x =>
        x.equals("") match {
          case true => 0
          case false => x.toDouble
        })
      parts
    }
      .filter(x => x(3) > 0 && x.last < 70)
      .map(x => x.head.toInt -> x.tail.last)
      .reduceByKey( (x, y) => x )// drop Minutes
      //.reduceByKey( (x, y) => (x, y).zipped.map(_ + _).dropRight(1) ++ Array(x(21)) ) // sum features with same Id
      //.map(_._2) // drop Id

    filteredData.sortByKey().coalesce(1).map(x => x._1 + "," + x._2).saveAsTextFile("Expected")
  }

}
