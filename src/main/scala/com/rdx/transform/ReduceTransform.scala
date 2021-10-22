package com.rdx.transform

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

object ReduceTransform {
  //主要的处理逻辑
  //1.读取文件中的数据
  //2.通过map对数据进行处理
  //3.封装对象，进行keyBy的处理
  //4.reduce处理
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)//方便测试
    val path = "C:\\Users\\thinkpad\\Desktop\\scalatest\\a\\source.txt"
    val source: DataStream[String] = env.readTextFile(path,"UTF-8")
    source.map(f => {
      val arr: Array[String] = f.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })
      .keyBy(f => f.id)
//      .reduce((v1,v2) => {//兰姆达表达式
//        SensorReading(v1.id,v2.ts,v1.temp.min(v2.temp))
//      })
      .reduce(new MyReduceFunction)
      .print()
    env.execute("myMin")

  }
}

//自定义reduce函数
class MyReduceFunction extends  ReduceFunction[SensorReading]{
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id,t1.ts,t.temp.min(t1.temp))
  }
}