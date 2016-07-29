package main.scala

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import java.io.File
import org.apache.spark.rdd.RDD
import scala.collection.mutable.Map
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.SVMWithSGD
import org.apache.spark.mllib.classification.SVMModel
import org.apache.spark.mllib.evaluation.binary.FMeasure

object Petunia {
  /**
   * @title Chuong trinh phan lop tap van ban lon theo chu de
   * @author TriNHD
   * @param0 duong dan den thu muc input, libs co dau splash, trong thu muc input co folder 0 va 1
   * @param1 can duoi (lower bound) cua chi so tfidf
   * @param2 can tren (upper bound) cua chi so tfidf
   * @param3 so luong tap train (vd: 0.7)
   */
  def main(args: Array[String]): Unit = {
    val startTime = System.currentTimeMillis()

    //~~~~~~~~~~~ Spark ~~~~~~~~~~~
    val conf = new SparkConf().setAppName("ISLab.Petunia").setMaster("spark://xla1:7077")
    val sc = new SparkContext(conf)
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    //~~~~~~~~~~~ Events ~~~~~~~~~~~
    var arrStopwords = new ArrayBuffer[String]
    var arrAttribute = new ArrayBuffer[String]
    var wordSet0 = new ArrayBuffer[Map[String, Int]]
    var wordSet1 = new ArrayBuffer[Map[String, Int]]

    //~~~~~~~~~~Get all data directories~~~~~~~~~~
    val inputDirPath = args(0) + "input"
    val input = (inputDirPath + File.separator + "0", inputDirPath + File.separator + "1")
    println(input._1 + "\n" + input._2)
    val bcInput1 = sc.broadcast(PUtils.getListOfSubFiles((new File(input._1))))
    val bcInput2 = sc.broadcast(PUtils.getListOfSubFiles((new File(input._2))))

    //~~~~~~~~~~Get all input files~~~~~~~~~~
    val listFiles0 = sc.parallelize(bcInput1.value)
    val listFiles1 = sc.parallelize(bcInput2.value)

    var wordSetByFile0: RDD[Map[String, Int]] = sc.emptyRDD[Map[String, Int]]
    //Foreach text file
    wordSetByFile0 = wordSetByFile0.union(listFiles0.map { fileDir =>
      PUtils.statWords(fileDir)
    })

    var wordSetByFile1: RDD[Map[String, Int]] = sc.emptyRDD[Map[String, Int]]
    //Foreach text file
    wordSetByFile1 = wordSetByFile1.union(listFiles1.map { fileDir =>
      PUtils.statWords(fileDir)
    })

    //~~~~~~~~~~Remove stopwords~~~~~~~~~~
    //// Load stopwords from file
    val stopwordFilePath = args(0) + "libs/vietnamese-stopwords.txt"
    val swSource = Source.fromFile(stopwordFilePath)
    swSource.getLines.foreach { x => arrStopwords.append(x) }
    swSource.close
    val bcStopwords = sc.broadcast(arrStopwords)
    //// Foreach document, remove stopwords
    wordSetByFile0.foreach(oneFile => oneFile --= bcStopwords.value)
    wordSetByFile1.foreach(oneFile => oneFile --= bcStopwords.value)
    val bcWordSet0 = sc.broadcast(wordSetByFile0.collect)
    val bcWordSet1 = sc.broadcast(wordSetByFile1.collect)

    //~~~~~~~~~~Calculate TFIDF~~~~~~~~~~
    var tfidfWordSet0: RDD[Map[String, Double]] = sc.emptyRDD[Map[String, Double]] // Map[word, TF*IDF-value]

    tfidfWordSet0 = tfidfWordSet0.union(wordSetByFile0.map(oneFile => {
      PUtils.statTFIDF(oneFile, bcWordSet0.value)
    }))

    var tfidfWordSet1: RDD[Map[String, Double]] = sc.emptyRDD[Map[String, Double]] // Map[word, TF*IDF-value]
    tfidfWordSet1 = tfidfWordSet1.union(wordSetByFile1.map(oneFile => {
      PUtils.statTFIDF(oneFile, bcWordSet1.value)
    }))
    tfidfWordSet0.cache
    tfidfWordSet1.cache

    //~~~~~~~~~~Normalize by TFIDF~~~~~~~~~~
    val lowerUpperBound = (args(1).toDouble, args(2).toDouble)
    println("Argument 0 (lower bound): " + lowerUpperBound._1 + " - Argument 1 (upper bound): " + lowerUpperBound._2)
    var attrWords: RDD[String] = sc.emptyRDD[String]
    attrWords = attrWords.union(tfidfWordSet0.flatMap(oneFile => {
      oneFile.filter(x => x._2 > lowerUpperBound._1 && x._2 < lowerUpperBound._2).keySet
    }))
    attrWords = attrWords.union(tfidfWordSet1.flatMap(oneFile => {
      oneFile.filter(x => x._2 > lowerUpperBound._1 && x._2 < lowerUpperBound._2).keySet
    }))

    //~~~~~~~~~~Create vector~~~~~~~~~~
    var vectorWords: RDD[LabeledPoint] = sc.emptyRDD[LabeledPoint]
    arrAttribute.appendAll(attrWords.collect)
    val bcAttrWords = sc.broadcast(arrAttribute)
    vectorWords = vectorWords.union(tfidfWordSet0.map(oneFile => {
      var vector = new ArrayBuffer[Double]
      bcAttrWords.value.foreach { word =>
        {
          if (oneFile.contains(word)) {
            vector.append(oneFile.get(word).get)
          } else vector.append(0d)
        }
      }
      LabeledPoint(0d, Vectors.dense(vector.toArray))
    }))
    vectorWords = vectorWords.union(tfidfWordSet1.map(oneFile => {
      var vector = new ArrayBuffer[Double]
      bcAttrWords.value.foreach { word =>
        {
          if (oneFile.contains(word)) {
            vector.append(oneFile.get(word).get)
          } else vector.append(0d)
        }
      }
      LabeledPoint(1d, Vectors.dense(vector.toArray))
    }))
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    val splits = vectorWords.randomSplit(Array(args(3).toDouble, 1 - args(3).toDouble), seed = 11L)
    val training = splits(0).cache()
    val test = splits(1)

    val numIterations = 100
    val model = SVMWithSGD.train(training, numIterations)

    val endTrain = System.currentTimeMillis();
    val durationTrain = (endTrain - startTime) / 1000;

    // Clear the default threshold.
    model.clearThreshold()
    //model.setThreshold(0.5)

    //Compute raw scores on the test set.
    val scoreAndLabels = test.map { point =>
      val score = model.predict(point.features)
      (score, point.label)
    }

    val endTest = System.currentTimeMillis();
    val durationTest = (endTest - startTime) / 1000;

    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
    //val auPR = metrics.areaUnderPR

    println("Include training duration: " + durationTrain + " seconds")
    println("Include testing duration: " + durationTest + " seconds")

    val precision = metrics.precisionByThreshold.collect.toMap[Double, Double]
    val recall = metrics.recallByThreshold.collect.toMap[Double, Double]
    val fMeasure = metrics.fMeasureByThreshold.collect.toMap[Double, Double]
    println("Threshold,Precision,Recall,F-Measure")
    precision.foreach(x => {
      println(x._1 + "," + x._2 + "," + recall.get(x._1).get + "," + fMeasure.get(x._1).get)
    })
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
  }
}