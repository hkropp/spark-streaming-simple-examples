package simpleexample

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

/*
  Submitting:
  spark-submit --master yarn-client \
       --num-executors 2 \
       --driver-memory 512m \
       --executor-memory 512m \
       --executor-cores 1 \
       --class simpleexample.SparkFileExample \
       spark-streaming-simple-example-0.1-SNAPSHOT.jar /spark_log
 */
object SparkFileExample {

  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      System.err.println("Usage: <log-dir>")
      System.exit(1)
    }

    val sparkConf = new SparkConf().setAppName("SpoolDirSpark")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val hiveContext = new HiveContext(ssc.sparkContext)
    import hiveContext.implicits._
    import hiveContext.sql

    val inputDirectory = args(0)

    val lines = ssc.fileStream[LongWritable, Text, TextInputFormat](inputDirectory).map{ case (x, y) => (x.toString, y.toString) }

    lines.print()

//    ToDo
//    lines.foreachRDD { rdd =>
//      rdd.foreachPartition { line =>
//        line.foreach { item =>
//          val values = item.toString().split(",")
//          val date = values(0)
//          val open = values(1)
//          val high = values(2)
//          val low = values(3)
//          val close = values(4)
//          val volume = values(5)
//          val adj_close = values(6)
//          val year = date.split("-")(0)
//          sql(f"INSERT INTO TABLE stocks PARTITION (year= '$year')  VALUES ('$date', $open, $high, $low, $close, $volume, $adj_close);")
//                     }
//                           }
//                     }

    ssc.start()
    ssc.awaitTermination()

  }
}

