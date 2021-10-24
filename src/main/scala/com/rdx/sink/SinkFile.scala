package com.rdx.sink

import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
  * 读取文件数据 readTextFile的使用
  * 通过转换的处理 map filter的使用
  * 输出到文件中 ： addSink的使用
  */
object SinkFile {
  def main(args: Array[String]): Unit = {
    //读取文件数据
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    lazy  val sourcePath = "C:\\Users\\thinkpad\\Desktop\\scalatest\\a\\source.txt"
    lazy  val sinkPath = "C:\\Users\\thinkpad\\Desktop\\scalatest\\sink\\filesink"
    val sourceData: DataStream[String] = env.readTextFile(sourcePath,"UTF-8")
    val resultData: DataStream[MySensorReading] = sourceData.map(data => {
      val arr: Array[String] = data.split(",")
      MySensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    }).filter(_.temp >= 30)


    resultData.addSink(
      StreamingFileSink.forRowFormat[MySensorReading](
        new Path(sinkPath),
        new SimpleStringEncoder[MySensorReading]("UTF-8")
      ).build()
    )

    env.execute("my sink")
  }
}

case class MySensorReading(id : String,ts:Long ,temp:Double)
