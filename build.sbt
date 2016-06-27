name := "kaggle"

version := "1.0"

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(

  "org.apache.spark" %% "spark-core" % "1.5.1" ,

  "org.apache.spark"  %% "spark-mllib" % "1.5.1",

  // https://mvnrepository.com/artifact/com.google.code.gson/gson
  "com.google.code.gson" % "gson" % "2.7"
)
    