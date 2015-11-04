name := "kaggle"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "1.5.1" ,

  "org.apache.spark"  %% "spark-mllib" % "1.5.1")
    