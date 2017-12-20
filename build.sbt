
name := "th-spark-debug"

version := "0.1"

// scalaVersion := "2.10.6"
scalaVersion := "2.11.11"

// sbt.version = 0.13.7, 在project/build.properties中配置

unmanagedBase := baseDirectory.value / "lib2"
// lib2中为spark2.2的jar包，spark2.0以上需要scala 2.11以上版本，jdk8支持


// val sparkStreamingKafka = "org.apache.spark" %% "spark-streaming-kafka" % "1.5.0-cdh5.5.1" % "provided"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming-kafka-0-8-assembly_2.11" % "2.2.0",
  "za.co.absa.spline" % "spline-core" % "0.2.3",
  "za.co.absa.spline" % "spline-persistence-mongo" % "0.2.3"
)
