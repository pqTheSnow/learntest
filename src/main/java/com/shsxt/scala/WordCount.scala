package com.shsxt.scala

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author qiong.peng
  * @Date 2019/10/21
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setMaster("local").setAppName("WordCount")


    val sc = new SparkContext(conf)

    val rdd: RDD[String] = sc.textFile("./src/main/resource/tmp/wd.txt")

    val result: Long = rdd.filter(x=>x.contains(",")).count()
    println("======================" + result)
    println("----------------------" + result.isInstanceOf[Long])

    sc.stop()

  }
}
