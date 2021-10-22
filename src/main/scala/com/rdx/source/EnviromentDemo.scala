package com.rdx.source

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.log4j.Logger

import scala.collection.immutable
import scala.util.Random
//定义样例类
case class SensorReading(id : String,timestamp : Long,temperature : Double)

object EnviromentDemo {
  private val LOG: Logger = Logger.getLogger("EnvormentDemo")
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //1.从集合中读取数据
    val list = List(
      SensorReading("sensor_1", 1634735342, 23.8),
      SensorReading("sensor_2", 1634735212, 39),
      SensorReading("sensor_5", 1634735389, 32),
      SensorReading("sensor_7", 1634734342, 31.8),
      SensorReading("sensor_6", 1634735378, 32.5)
    )
    val source: DataStream[SensorReading] = env.fromCollection(list)
    source.print()

    env.execute()
  }
}

/**
  * 读取txt文件
  */
object SourceOfText{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val path : String = "C:\\Users\\thinkpad\\Desktop\\scalatest\\a\\source.txt"
    val source: DataStream[String] = env.readTextFile(path)


    source.print()
    env.execute()
  }
}


//自定义source
object MySource{
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val source: DataStream[String] = env.addSource(new MySensorSource)
    source.print()

    env.execute()
  }
}

class MySensorSource extends SourceFunction[String]{
  var running : Boolean = true
  override def cancel(): Unit = {
    running = false
  }

  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    //定义随机数
    val strings: immutable.IndexedSeq[String] = 1.to(10).map(f => {
      s"sensor_${f},${Random.nextDouble() * 100}"
    })

    while(running){
      strings.map(f =>{
        val arr: Array[String] = f.split(",")
        s"sensor_${arr(0)},${arr(1)+Random.nextGaussian()}"
      })

      strings.foreach(f =>{
        sourceContext.collect(s"${f} + ,${System.currentTimeMillis()}")
      })

      Thread.sleep(500)
    }
  }

}