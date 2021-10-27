package com.rdx.transform.window

import java.time.Duration

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, TimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * 1.获取socket流中的数据
  * 2.然后指定watermark
  *3.进行keyBy操作
  * 4.window操作
  * 对迟到的数据进行侧输出流的操作
  * 5.聚合reduce
  */

object WindowAndWaterMark {
  def main(args: Array[String]): Unit = {
    val late = new OutputTag[SensorReading2]("late")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //设置事件时间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //设置waterMark的延迟时间
    val watermarkStrategy: WatermarkStrategy[SensorReading2] = WatermarkStrategy.forBoundedOutOfOrderness[SensorReading2](Duration.ofSeconds(2))
      .withTimestampAssigner(new SerializableTimestampAssigner[SensorReading2] {
        override def extractTimestamp(element: SensorReading2, recordTimestamp: Long): Long = element.ts*1000
      })

    val sourceData: DataStream[SensorReading2] = env.socketTextStream("localhost",6666).map(f =>{
      val arr: Array[String] = f.split(",")
      SensorReading2(arr(0),arr(1).toLong,arr(2).toDouble)
    })
    val result: DataStream[SensorReading2] = sourceData.assignTimestampsAndWatermarks(watermarkStrategy)
      .keyBy(f => f.id)
      //进行滚动时间窗口的设置
      .window(TumblingEventTimeWindows.of(Time.milliseconds(15)))
      //设置延迟数据的的时间
      .allowedLateness(Time.seconds(10)) //设置迟到数据的延迟时间
      //迟到的时间进行侧输出流操作
      .sideOutputLateData(late)
      .reduce((a, b) => SensorReading2(a.id, b.ts, a.temp.min(b.temp)))

    result.print("正常数据")
    result.getSideOutput(late).print("late")
    env.execute("my late job")

  }
}
