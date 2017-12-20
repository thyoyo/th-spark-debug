import org.apache.commons.configuration.{Configuration, PropertiesConfiguration}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.sql.{SaveMode, SparkSession}
import za.co.absa.spline.core.conf.DefaultSplineConfigurer


/**
  * Created by tian on 2017/11/23
  *
  *
  */
object TestSpline {

  def main(args: Array[String]) = {

    /**
      * https://absaoss.github.io/spline/
      * 仅支持spark 2.2使用，需要使用sparksession
      *
      * 1. mongo 服务启动
      * 2. 运行UI WAR包（java8运行）：
      * java -jar /Users/tianhua/Documents/code_yiche/th-spark-debug/bin/spline-web-0.2.3-exec-war.jar
      *      -Dspline.mongodb.url=mongodb://127.0.0.1:27017
      *      -Dspline.mongodb.name=myTest
      * */
    val sparkBuilder = SparkSession.builder().master("local").appName("Sample Job 2")
    val spark = sparkBuilder.getOrCreate()

    // Enable data lineage tracking with Spline
    import za.co.absa.spline.core.SparkLineageInitializer._
    val config_file = "/Users/tianhua/Documents/code_yiche/th-spark-debug/src/main/resource/spline.properties"
    spark.enableLineageTracking(new DefaultSplineConfigurer(new PropertiesConfiguration(config_file)))

    // A business logic of a Spark job ...
    import spark.implicits._

    val sourceDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("file:///Users/tianhua/Documents/code_yiche/th-spark-debug/data/wikidata.csv")
        .as("source")
        .filter($"total_response_size" > 1000)
        .filter($"count_views" > 10)

    val domainMappingDS = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv("file:///Users/tianhua/Documents/code_yiche/th-spark-debug/data/domain.csv")
        .as("mapping")

    val joinedDS = sourceDS
        .join(domainMappingDS, $"domain_code" === $"d_code", "left_outer")
        .select($"page_title".as("page"), $"d_name".as("domain"), $"count_views")

    // joinedDS.write.mode(SaveMode.Overwrite).parquet("file:///Users/tianhua/Downloads/job1_results")
    joinedDS.write.mode(SaveMode.Overwrite).json("file:///Users/tianhua/Documents/code_yiche/th-spark-debug/data/job1_results")

  }

}
