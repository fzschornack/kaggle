import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by fzschornack on 09/01/16.
 */
object HomesiteQuoteConversion {

  def main(args: Array[String]) {
    val trainFile = "homesite_quote/train.csv"
    val testFile = "homesite_quote/test.csv"

    val trainExampleFile = "homesite_quote/train_example.csv"
    val testExampleFile = "homesite_quote/test_example.csv"

    val conf = new SparkConf().setAppName("Homesite Quote Conversion").setMaster("local")
    val sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.WARN)
  }



}
