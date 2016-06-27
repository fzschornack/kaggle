import com.google.gson.Gson
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._

case class AngelJob(name: String,
                    juniorCompensation: Double,
                    seniorCompensation: Double,
                    jobType: Array[String],
                    city: String,
                    tags: Array[String],
                    country: String,
                    countryShortName: String,
                    lat: Double,
                    lng: Double,
                    costPerYear: Double,
                    rentPerYear: Double)

/**
  * Created by fzschornack on 25/06/2016.
  */
object AngelDataVis {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Angel Jobs Data Process").setMaster("local[4]")
    val sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.WARN)

    val citiesFile = sc.textFile("angel/angel_cities_coordinates.csv")

    val mapCities = citiesFile.map { line =>
      val parts = line.split(",") // [0:city, 1:countryShortName, 2:countryLongName, 3:lat, 4:lng]
      (parts.head, Array(parts(1), parts(2), parts(3).toDouble, parts(4).toDouble))
    }.collectAsMap()

    val citiesCostFile = sc.textFile("angel/cities-cost-living-rent.csv")

    val mapCitiesCost = citiesCostFile.map { line =>
      val parts = line.split(",") // [0:city, 1:cost, 2:rent]
      (parts(0), Array(parts(1).toDouble, parts(2).toDouble))
    }.collectAsMap()

    val countriesCostFile = sc.textFile("angel/countries-cost-living-rent.csv")

    val mapCountriesCost = countriesCostFile.map { line =>
      val parts = line.split(",") // [0:country, 1:cost, 2:rent]
      (parts(0), Array(parts(1).toDouble, parts(2).toDouble))
    }.collectAsMap()

    val rawFilePath = "angel/angel-jobs-info-all.csv"

    val rawData = sc.textFile(rawFilePath)

    val processedData = rawData.map { line =>
      val parts = line.split(",") // [0:job_name, 1:compensation, 2:equity, 3:type, 4:city, 5:tags]

      val compensationSplit = parts(1).split(" - ")
      val juniorCompensationRaw = compensationSplit(0).replaceAll("""(k|\s+)""","")
      val seniorCompensationRaw = compensationSplit(1).replaceAll("""(k|\s+)""","")

      var juniorCompensationDouble = 0.0
      var seniorCompensationDouble = 0.0

      // Indian Rupee - ₹
      if (juniorCompensationRaw.contains("₹")) {
        juniorCompensationDouble = juniorCompensationRaw.replace("₹", "").toDouble * 0.015
        seniorCompensationDouble = seniorCompensationRaw.replace("₹", "").toDouble * 0.015
      }

      // Euro - €
      else if (juniorCompensationRaw.contains("€")) {
        juniorCompensationDouble = juniorCompensationRaw.replace("€", "").toDouble * 1.11
        seniorCompensationDouble = seniorCompensationRaw.replace("€", "").toDouble * 1.11
      }

      // British Pounds - £
      else if (juniorCompensationRaw.contains("£")) {
        juniorCompensationDouble = juniorCompensationRaw.replace("£", "").toDouble * 1.37
        seniorCompensationDouble = seniorCompensationRaw.replace("£", "").toDouble * 1.37
      }

      // Japanese Yen - ¥
      else if (juniorCompensationRaw.contains("¥")) {
        juniorCompensationDouble = juniorCompensationRaw.replace("¥", "").toDouble * 1.37
        seniorCompensationDouble = seniorCompensationRaw.replace("¥", "").toDouble * 1.37
      }

      // USD - $
      else {
        try {
          juniorCompensationDouble = juniorCompensationRaw.replace("$", "").toDouble
          seniorCompensationDouble = seniorCompensationRaw.replace("$", "").toDouble
        } catch {
          case e : NumberFormatException => {} // skip
        }
      }

      val name = parts(0)
      val jobType = parts(3).split(" - ")
      val city = parts(4)
      val tags = parts(5).split(" · ")

      var country = "null"
      var countryShortName = "null"
      var lat = -360.0
      var lng = -360.0

      // mapCities: (city, [0:countryShortName, 1:countryLongName, 2:lat, 3:lng])
      try {
        val cityInfo = mapCities(city)
        country = cityInfo(1).asInstanceOf[String]
        countryShortName = cityInfo(0).asInstanceOf[String]
        lat = cityInfo(2).asInstanceOf[Double]
        lng = cityInfo(3).asInstanceOf[Double]
      } catch {
        case e : NoSuchElementException => {} // skip
      }

      // mapCitiesCost: (city, [0:cost, 1:rent])
      var costPerYear = -1.0;
      var rentPerYear = -1.0;

      try {
        val cityCostInfo = mapCitiesCost(city)
        costPerYear = cityCostInfo(0).asInstanceOf[Double] * 12
        rentPerYear = cityCostInfo(1).asInstanceOf[Double] * 12
      } catch {
        case e : NoSuchElementException => {
          // try country cost
          try {
            val countryCostInfo = mapCountriesCost(country)
            costPerYear = countryCostInfo(0).asInstanceOf[Double] * 12
            rentPerYear = countryCostInfo(1).asInstanceOf[Double] * 12
          } catch {
            case e : NoSuchElementException => {} // skip
          }
        }
      }

      AngelJob(name,
        juniorCompensationDouble,
        seniorCompensationDouble,
        jobType,
        city,
        tags,
        country,
        countryShortName,
        lat,
        lng,
        costPerYear,
        rentPerYear)

    }

//    println(processedData.map(new Gson().toJson(_)).collect().mkString(","))

//    processedData.flatMap( job => job.tags).countByValue().toArray.sortBy(_._2).foreach(println)

    processedData.flatMap( job => job.jobType).countByValue().toArray.sortBy(_._2).foreach(println)

//    val cities = processedData.map(job => "'" + job.city + "'").distinct().collect().sorted
//    println(cities.mkString(","))

//    processedData.foreach(println)


  }

}
