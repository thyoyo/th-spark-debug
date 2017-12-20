import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.kafka.KafkaUtils


/**
  * Created by tian on 2017/11/23
  *
  *
  */
object Test {

  def main(args: Array[String]) = {

    val sparkConf = new SparkConf().setAppName("Test").setMaster("local")

    /*
    val sc = new SparkContext(sparkConf)
    val rdd = sc.parallelize(1 to 10)
    rdd.collect().foreach(r => {
      System.out.println(r.toString)
    })
    * */


    val zkQuorum = "ingest3.bitauto.dmp:2181/kafka"
    val group = "autodsp_test_111"
    val topics = "autodsp_test"
    val numThreads = 2
    val interval = 360
    val ssc = new StreamingContext(sparkConf, Seconds(interval))
    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap //日志在Kafka中的topic及其分区
    val stream = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2)
    stream.foreachRDD( rdd => rdd.foreachPartition(
      p => {
        p.foreach(
          record => {
            System.out.println(record.toString)
          }
        )
      }
    ))

    ssc.start()
    ssc.awaitTermination()


  }
}
