import org.apache.log4j.{Level, LogManager}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by fzschornack on 15/07/2016.
  */
object Bimbo {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Bimbo").setMaster("local[*]")
    val sc = new SparkContext(conf)

    LogManager.getRootLogger.setLevel(Level.WARN)

    val bimboData = sc.textFile("kaggle/bimbo/train.csv")

    val bimboDataArray = bimboData.map { line =>
      val parts = line.split(",")
      (parts(1),parts(3),parts(4),parts(5),parts.last)
    }//.filter( v => v._5.toInt <= 50)

    //return (agencia:ruta:clienteId:productId, demanda_mean)
    val baseRDD = bimboDataArray.map( v => (v._1+':'+v._2+':'+v._3+':'+v._4, v._5.toFloat))
      .combineByKey(
        (v) => (Math.log(v+1).toFloat, 1),
        (acc: (Float, Int), v) => (acc._1 + Math.log(v+1).toFloat, acc._2 + 1),
        (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )


    //return (agencia:clienteId:productId, demanda_mean)
    val cliente_productid_demanda = baseRDD.map { v =>
      val parts = v._1.split(":") // {0:agencia, 1:ruta, 2:cliente, 3:produto}
      (parts(2) + ":" + parts(3), v._2)
    }.reduceByKey( (v1,v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .mapValues(v => Math.exp(v._1 / v._2.toFloat).toInt - 1)

//    //return (rutaSak:productId, demanda_mean)
//    val ruta_productid_demanda = baseRDD.map { v =>
//      val parts = v._1.split(":") // {0:agencia, 1:ruta, 2:cliente, 3:produto}
//      (parts(1) + ':' + parts(3), v._2)
//    }.reduceByKey( (v1,v2) => (v1._1 + v2._1, v1._2 + v2._2))
//      .mapValues(v => Math.exp(v._1 / v._2).toFloat - 1).collectAsMap()

    //return (agenciaId:productId, demanda_mean)
    val agencia_productid_demanda = sc.broadcast(baseRDD.map { v =>
      val parts = v._1.split(":") // {0:agencia, 1:ruta, 2:cliente, 3:produto}
      (parts(0) + ':' + parts(3), v._2)
    }.reduceByKey( (v1,v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .mapValues(v => Math.exp(v._1 / v._2.toFloat).toInt - 1).collect())

    //return (productId, demanda_mean)
    val productid_demanda = baseRDD.map { v =>
      val parts = v._1.split(":") // {0:agencia, 1:ruta, 2:cliente, 3:produto}
      (parts(3), v._2)
    }.reduceByKey( (v1,v2) => (v1._1 + v2._1, v1._2 + v2._2))
      .mapValues(v => Math.exp(v._1 / v._2.toFloat).toInt - 1)

    val globalMean = productid_demanda.filter( t => t._2 <= 50).map( t => t._2).mean()

    val productid_demandaMap = sc.broadcast(productid_demanda.collect())

    /////////////////////// TEST ////////////////////////////////////////////////////
    val bimboTest = sc.textFile("kaggle/bimbo/test.csv")

    //return (testId, agenciaId, productId)
    val submission = bimboTest.map { line =>
      val parts = line.split(",") // {0:id, 1:semana, 2:agencia, 3:canal, 4:ruta, 5:cliente, 6:produto}
      (parts(5)+":"+parts(6),parts)
    }.leftOuterJoin(cliente_productid_demanda)
      .map{ parts =>
        if (parts._2._2 != None) {
          (parts._2._1(0), parts._2._2.get)
        } else {
          val idx2 = agencia_productid_demanda.value.indexWhere(v => v._1.equals(parts._2._1(2) + ":" + parts._2._1(6)))
          if (idx2 != -1) {
            val demandaAgenciaProduto = agencia_productid_demanda.value.apply(idx2)._2
            (parts._2._1(0), demandaAgenciaProduto)
          } else {
            //      val demandaRutaProduto = ruta_productid_demanda.get(parts(4)+":"+parts(6))
            val idx3 = productid_demandaMap.value.indexWhere(v => v._1.equals(parts._2._1(6)))
            if (idx3 != -1) {
              val demandaProduto = productid_demandaMap.value.apply(idx3)._2
              (parts._2._1(0), demandaProduto)
            } else {
              (parts._2._1(0), globalMean)
            }
          }
        }
    }.coalesce(1,true).map(v => v._1+","+v._2).saveAsTextFile("kaggle/bimbo/submission_agencia_cliente_ruta")

  }

}
