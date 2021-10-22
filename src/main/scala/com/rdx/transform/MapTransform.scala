package com.rdx.transform

import org.apache.flink.streaming.api.scala._
case class SensorReading(id:String,ts:Long,temp:Double)
object MapTransform {
  //主要的处理逻辑
  //1.读取文件中的数据
  //2.通过map对数据进行处理
  //3.封装对象，进行keyBy的处理
  //4.求出最小值
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val path = "C:\\Users\\thinkpad\\Desktop\\scalatest\\a\\source.txt"
    val sourceDS: DataStream[String] = env.readTextFile(path,"UTF-8")
    sourceDS.map(f => {
      val arr: Array[String] = f.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
      .keyBy(f => f.id)
        .minBy("temp")
//      .min("temp")
        .print()
    env.execute("myMin")

  }
}
