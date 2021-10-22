package com.rdx.transform

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutPut_Transform {
  /**
    * 对数据流进行分流的处理
    * sideoutput 的使用
    * @param args
    */
  def main(args: Array[String]): Unit = {
    //定义测流输出标记
    val tag1 =  OutputTag[String]("heigh")
    val tag2 =  OutputTag[String]("lower")
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

//    val source : DataStream[Long] = env.fromElements[Long](1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)
    val source: DataStream[Long] = env.generateSequence(1,30)
    /*自定义测流输出的函数类*/
//    val process: DataStream[String] = source.process(new MyProcess(tag1,tag2))

    /*直接通过处理的函数类进行实现*/
    val process: DataStream[String] = source.process(new ProcessFunction[Long, String] {
      override def processElement(i: Long, context: ProcessFunction[Long, String]#Context, collector: Collector[String]): Unit = {
        if (i % 2 == 0) context.output[String](tag1, s"偶数_${i}") else context.output[String](tag2, s"奇数_${i}")
      }
    })
        process.getSideOutput(tag1).print("hight")
        process.getSideOutput(tag2).print("low")
    env.execute("job")
  }
}

/**
  * 自定义测流输出函数
  * @param tag1
  * @param tag2
  */
class MyProcess( tag1 : OutputTag[String],tag2 : OutputTag[String] ) extends  ProcessFunction[Long,String]{

  override def processElement(i: Long, context: ProcessFunction[Long, String]#Context, collector: Collector[String]): Unit = {
    if( i % 2 == 0 ) context.output(tag1,"偶数：" + i) else context.output(tag2,"奇数：" + i)
    collector.collect(i+"")
  }
}