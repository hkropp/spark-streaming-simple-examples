package simpleexample

import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaUtils}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

/**
 * Created by hkropp on 19/04/15.
 */
object SparkKafkaExample {

  def main(args: Array[String]): Unit = {
    if(args.length < 2) {
      System.err.println("Usage: <broker-list> <zk-list> <topic>")
      System.exit(1)
    }

    val Array(broker, zk, topic) = args

    val sparkConf = new SparkConf().setAppName("KafkaHBaseWordCount")
    val ssc = new StreamingContext(sparkConf, Seconds(2))
    ssc.checkpoint("./checkpoints")       // checkpointing dir
//    ssc.checkpoint("hdfs://checkpoints")  // dir in hdfs for prod

    val kafkaConf = Map("metadata.broker.list" -> broker,
                        "zookeeper.connect" -> zk,
                        "group.id" -> "kafka-spark-streaming-example",
                        "zookeeper.connection.timeout.ms" -> "1000")

    /* Kafka integration with reciever */
//    val lines = KafkaUtils.createStream[Array[Byte], String, DefaultDecoder, StringDecoder](
//      ssc,
//      kafkaConf,
//      Map(topic -> 1),
//      StorageLevel.MEMORY_ONLY_SER).map(_._2)
    /* Experiemental DirectStream w/o Reciever */
    val lines = KafkaUtils.createDirectStream[Array[Byte], String, DefaultDecoder, StringDecoder](
      ssc,
      kafkaConf,
      Set(topic)).map(_._2)
    /* Getting Kafka offsets from RDDs */
    lines.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      offsetRanges.foreach( println(_) )
    }                         }
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKeyAndWindow(_ + _, _ - _, Minutes(5), Seconds(2), 2)

    wordCounts.print()

    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", "localhost:2181")
    conf.set("hbase.mapred.outputtable", "stream_count")
    conf.set("hbase.mapreduce.scan.column.family", "word")
    conf.set("hbase.zookeeper.quorum", "localhost");
    conf.set("hbase.zookeeper.property.clientPort", "2181");
    conf.set("hbase.master", "localhost:60000");
    conf.set("hbase.rootdir", "file:///tmp/hbase")
    conf.set("hbase.zookeeper.property.dataDir", "/tmp/zookeeper")

    wordCounts.saveAsNewAPIHadoopFiles(
      "_tmp",
      "_result",
      classOf[Text],
      classOf[IntWritable],
      classOf[org.apache.hadoop.hbase.mapreduce.TableOutputFormat[Text]],
      conf)

    ssc.start()
    ssc.awaitTermination()
  }


}
