package com.rdx.transform

import org.apache.flink.streaming.api.scala._

object SplitAndSelect {
  /**
    * 将数据流进行拆分，
    * 通过split select 进行数据的拆分
    * 然后print输出结果
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val path = "C:\\Users\\thinkpad\\Desktop\\scalatest\\a\\source.txt"
    val sourceDS: DataStream[String] = env.readTextFile(path,"UTF-8")
    val splitStream: SplitStream[MySensorReading] = sourceDS.map(data => {
      val arr: Array[String] = data.split(",")
      MySensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    }).split(f => {
      if (f.temp > 28) Seq("height") else Seq("lower")
    })
    splitStream.select("height").print("hight")
    splitStream.select("lower").print("lower")

    env.execute("myjob")

  }
}

case class MySensorReading(id:String , ts : Long , temp : Double)
