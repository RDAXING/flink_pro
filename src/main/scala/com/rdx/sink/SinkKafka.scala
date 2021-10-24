package com.rdx.sink

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

/**
  * 读取文件数据写入到kafka
  */

case class MySensorReading1(id : String,ts : Long, temp : Double)

object SinkKafka {
  def main(args: Array[String]): Unit = {
    val sourcePath = "C:\\Users\\thinkpad\\Desktop\\scalatest\\a\\source.txt"

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sourceData: DataStream[String] = env.readTextFile(sourcePath,"UTF-8")

    val result: DataStream[String] = sourceData.map(f => {
      val data: Array[String] = f.split(",")
      MySensorReading1("转换：" + data(0), data(1).toLong, data(2).toDouble).toString
    })
    result.addSink(new FlinkKafkaProducer011[String]("dn1.hadoop:9092,dn2.hadoop:9092,dn3.hadoop:9092","rdx_demo",new SimpleStringSchema()))
    env.execute("my kafka sink")
  }
}

/*
* 获取kafka中的数据，存放到kafka中
* */

object KafkaSinkToSkafka{
  def main(args: Array[String]): Unit = {
    //kafka produce配置
    val properties = new Properties()
    properties.put("bootstrap.servers","dn1.hadoop:9092,dn2.hadoop:9092,dn3.hadoop:9092")
    properties.put("group.id","group_fk")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("auto.offset.reset","earliest")

    //获取flink环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val soureData: DataStream[String] = env.addSource(new FlinkKafkaConsumer011[String]("nihao",new SimpleStringSchema(),properties))


    val result: DataStream[String] = soureData.map(f => {
      val data: Array[String] = f.split(",")
      MySensorReading("kafka to kafka" + data(0), data(1).toLong, data(2).toDouble).toString
    })


    result.addSink(new FlinkKafkaProducer011[String]("dn1.hadoop:9092,dn2.hadoop:9092,dn3.hadoop:9092","rdx_demo",new SimpleStringSchema()))

    env.execute("kafka to kafka")
  }
}



