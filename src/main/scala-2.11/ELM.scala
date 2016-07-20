import org.apache.spark.mllib.linalg.{Vectors, Matrices}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD

import java.util.Random

import org.apache.spark.storage.StorageLevel

/**
 * Object to implement the "most creative problem solving" machine learning algorithm: Extreme Learning Machine
 *
 * Usage:
 *
 *  Train
 *  1) originalLabeledPoints
 *  2) newLabeledPoints = ELM.transform(originalLabeledPoints, 200, 1)
 *  3) use newLabeledPoints to train a model
 *
 *  Prediction
 *  4) originalPredictionLabeledPoints
 *  5) newPredictionLabeledPoints = ELM.transform(originalPredictionLabeledPoints, 200, 1) // MUST BE THE SAME
 *                                                                                         // SEED USED FOR TRAINING!!!
 *  6) model.predict(newPredictionLabeledPoints)
 *  7) open a champagne bottle, sit and relax.
 *
 * Created by fzschornack on 24/11/15.
 */
object ELM {

  /**
   * Transform input features in new hiddenLayerSize features using a non linear function.
   *
   * @param input
   * @param hiddenLayerSize
   * @return
   */
  def transformation(input: RDD[LabeledPoint], hiddenLayerSize: Int, seed: Int): RDD[LabeledPoint] = {
    val numberOfFeatures = input.take(1).apply(0).features.size

    val inputToHiddenLayerWeights = Matrices.randn(hiddenLayerSize, numberOfFeatures , new Random(seed))

    inputToHiddenLayerWeights.toArray.foreach(println)

    val elmLabeledPoints = input.map { lp =>

      val label = lp.label
      val elmFeatures = inputToHiddenLayerWeights.multiply(lp.features) // wx
        .values//.map( x => logisticFunction(math.log10(x))) // g(wx)

      LabeledPoint(label, Vectors.dense(Array(-1.0) ++ elmFeatures))

    }

    elmLabeledPoints
  }

  private def logisticFunction(wx: Double): Double = {
    1 / (1 + math.exp(-wx))
  }

  private def tanhFunction(wx: Double): Double = {
    (math.exp(wx) - math.exp(-wx)) / (math.exp(wx) + math.exp(-wx))
  }

  private def rectifiedLinearFunction(wx: Double): Double = {
    math.max(0, wx)
  }

}
