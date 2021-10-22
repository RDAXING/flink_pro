package com.rdx.transform

import org.apache.flink.streaming.api.scala._

/**
  * 两条不同的流进行连接
  * 注意：两条流可以相同，也可以不同
  *
  * connect CoMap
  */
object ConnectAndCoMap {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val source1: DataStream[Long] = env.generateSequence(1 ,10)
    val source2: DataStream[String] = env.fromElements("a","b","c")
    val result: DataStream[String] = source1.connect(source2).map(
      s1 => s"source1_${s1}",
      s2 => s"source2:${s2}"
    )
    result.print("connect")
    env.execute("myjob")
  }
}

/**
  * 数据类型必须一致
  * stream1.union(stream2)
  */
object union {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val source1: DataStream[Int] = env.fromElements(1,2,3)
    var source2: DataStream[Int] = env.fromElements(4,5,6)
     source2 = source1.union(source2)
    source2.print("result")
    env.execute("myjob")

  }
}
