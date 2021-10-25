package com.rdx.transform.window

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 时间窗口的定义
  * 1.获取socket中的数据
  * 2.进行map处理封装SensorReading类
  * 3.进行keyBy分组
  * 4.进行开窗操作（TimeWindow）
  * 5.进行聚合操作（reduce/min）
  */
case class SensorReading2(id:String,ts:Long,temp:Double)
object TimeWindowOfTuble {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceData: DataStream[String] = env.socketTextStream("localhost",6666)

    val resultData: DataStream[SensorReading2] = sourceData.map(f => {
      val arr: Array[String] = f.split(",")
      SensorReading2(arr(0), arr(1).toLong, arr(2).toDouble)
    }).keyBy(f => f.id)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
      //      .min("temp")//按照指定属性进行分组求最小值
      .reduce((curdata, newData) => {//reduce的使用，可以是兰姆达表达式，也可以是自己定义函数类
      SensorReading2(curdata.id, newData.ts, curdata.temp.min(newData.temp))
    })

    resultData.print()

    env.execute("my window job")

  }
}

/**
  * 自定义reduce函数类，求出最小温度以及最新的时间
  */
class MyReduceFunction extends ReduceFunction[SensorReading2]{
  override def reduce(value1: SensorReading2, value2: SensorReading2): SensorReading2 = {
    SensorReading2(value1.id,value2.ts,value1.temp.min(value2.temp))
  }
}

