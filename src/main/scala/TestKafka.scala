import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}
import java.util.Date
import java.util.concurrent.Executors

import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.slf4j.LoggerFactory
import _root_.kafka.serializer.StringDecoder

/**
  * Created by tian on 2017/11/23
  *
  */
object TestKafka {
  val log = LoggerFactory.getLogger(TestKafka.getClass)

  def main(args: Array[String]) {
    val conf = new SparkConf().setMaster("local[*]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(2))

    val topics = "YcAppStatisticsLog"
    val topicsSet = topics.split(",").toSet
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "ingest0.bitauto.dmp:9092,ingest3.bitauto.dmp:9092,ingest4.bitauto.dmp:9092, ingest5.bitauto.dmp:9092")
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)

    val aa = messages
        .mapPartitions(partition => partition.map(row => (row._1.length, row._2.length)))
        .filter(row => row._1 > 1).cache

    val bb = aa.mapPartitions(p => p)
        .reduceByKey(_ + _)
        .map(r => r)
        .groupByKey()
        .mapValues(row => row.toArray.sum)
        .map(r => r)
        .map(r => r)
        .map(r => r)


    val cc = aa.groupByKey()
        .mapValues(r => r.toArray.sum)
        .join(bb)
        .map(r => r)

    cc.foreachRDD(rdd => {
      rdd.foreachPartition(
        rows => {
          while (rows.hasNext) {
            val line = rows.next()
            (line._1, line._2)
          }
        }
      )
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
