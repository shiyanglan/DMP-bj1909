package com.qf.dmp.context

import java.util.Properties

import com.qf.dmp.etl.Log2Parquet
import com.qf.dmp.utils.{Spark_utils, TagsUtils}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{Edge, Graph}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 *  标签上下文，用于合并所有的标签
 */
object TagsContextV2 {
    def main(args: Array[String]): Unit = {
        val SPARK_PROPERTIES:String = "spark.properties"
        //1. 控制读取和存储的参数
        if(args == null || args.length != 5) {
            println("Usage : <input> <output> <dic> <stopWords> <day>")
            System.exit(-1)
        }
        val Array(input, output, dic, stopWords, day) = args
        //2. 获取到入口并配置序列化以及压缩方式
        val properties = new Properties()
        properties.load(Log2Parquet.getClass.getClassLoader.getResourceAsStream(SPARK_PROPERTIES))
        val spark = Spark_utils.getLocalSparkSession(TagsContextV2.getClass.getSimpleName)
        spark.sqlContext.setConf(properties)


        //3. 获取到字典(url,app)
        //3.1 媒体字典
        val dicMap = spark.sparkContext.textFile(dic)
            .map(_.split("\t", -1)).filter(_.length >= 5)
            .map(arr => (arr(4), arr(1))).collectAsMap()
        //3.2 关键字字典
        val wordMap = spark.sparkContext.textFile(stopWords).map((_,0)).collectAsMap()

        //4. 广播变量
        //4.1 媒体广播变量
        val broadcastDic: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(dicMap)
        //4.2 关键字广播变量
        val broadcastWord: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(wordMap)

        //5. 读取日志数据
        val df = spark.read.parquet(input)

        //===========================================================================================
        //6. 处理数据打标签
        //6.1 获取到所有的userid并把它封装到RDD中
        val baseRDD: RDD[(List[String], Row)] = df.filter(TagsUtils.onUserId).rdd.map(row => {
            //6.1 获取到所有的非空userid
            val allUsers: List[String] = TagsUtils.getAnyAllUserId(row)
            (allUsers, row)
        })


        //6.2 构建点和边
        //6.2.1 构建点
        val vd = baseRDD.flatMap(tp => {
            val row = tp._2 // 获取row

            val adTag = TagsAd.makeTags(row) // 广告标签
            val appTag = TagsApp.makeTags(row, broadcastDic) //媒体标签
            val platformTag = TagsPlatform.makeTags(row)//渠道标签
            val devTag = TagsDev.makeTags(row) //设备标签
            val keyWordTag = TagsKeyWord.makeTags(row, broadcastWord)  //关键字标签
            val bussnessTag = TagsBussness.makeTags(row)   // 商圈标签

            val tags = adTag ++ appTag ++ platformTag ++ devTag ++ keyWordTag ++ bussnessTag // 拼接数据
            val userTagsList = tp._1.map((_, 0)) ++ tags

            // 为了保证数据准确性，只能在其中一个userid中打标签，而其他的userid不携带标签，我们这里使用list的第一个userid打标签
            tp._1.map(uid => {
                if (tp._1.head.equals(uid)) (uid.hashCode.toLong, userTagsList) // 第一次遍历这句化肯定执行
                else (uid.hashCode.toLong, List.empty) // 除了第一个userid，其他的没有携带标签
            })
        })

        //6.2.2 构建边
        val edges = baseRDD.flatMap(tp => {
            tp._1.map(uid => Edge(tp._1.head.hashCode.toLong, uid.hashCode.toLong, 0))
        })

        //7. 生成图
        val graph = Graph(vd, edges)
        val vertices = graph.connectedComponents().vertices


        vertices.join(vd).map {
            case (uid, (cmid, tags)) => (cmid, tags)
        }.reduceByKey((list1, list2) => (list1:::list2).groupBy(_._1).mapValues(_.foldLeft[Int](0)(_+_._2)).toList)
            .take(20).foreach(println)

        //8. 释放资源
        Spark_utils.stop(spark)
        //===========================================================================================
    }
}
