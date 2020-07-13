/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TegsContext
 * Author: yanglan88
 * Date: 2020/6/24 11:11
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/24
 * @since 1.0.0
 */
package com.qf.dmp.context

import java.util.Properties

import com.qf.dmp.etl.Data2Json.SPARK_PROPERTIES
import com.qf.dmp.traits.Logger_Trait
import com.qf.dmp.utils.{Spark_utils, TagsUtils}
import com.typesafe.config.ConfigFactory
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}

object TegsContext extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        if (args == null || args.length != 5) {
            println("Usage : <input> <output> <dev> <stopWords> <day>")
            System.exit(-1)
        }
        val Array(input , output , dic , stopWords,day) = args



        val properties = new Properties()
        properties.load(TegsContext.getClass.getClassLoader.getResourceAsStream(SPARK_PROPERTIES))

        val spark: SparkSession = Spark_utils.getLocalSparkSession(TegsContext.getClass.getName)
        spark.sqlContext.setConf(properties)



        //**************************************************************************
        //x.1 配置hbase
        val load = ConfigFactory.load // 默认就会去加载classpath下的叫做application.properties
        val hbaseTableName = load.getString("hbase.table.name")
        val configuration = spark.sparkContext.hadoopConfiguration
        configuration.set("hbase.zookeeper.quorum", load.getString("hbase.zookeeper.host"))

        //x.2 获取连接
        val hbConn = ConnectionFactory.createConnection(configuration)
        val hbAdmin = hbConn.getAdmin

        //x.3 先判断表是否存在
        if(!hbAdmin.tableExists(TableName.valueOf(hbaseTableName))) {
            println("表不存在，可创建")
            //x.4 创建表对象
            val tableName = new HTableDescriptor(TableName.valueOf(hbaseTableName))
            val columnFamily = new HColumnDescriptor("tags")
            tableName.addFamily(columnFamily)
            hbAdmin.createTable(tableName)
            hbAdmin.close()
            hbConn.close()
        }

        //x.4 创建hadoop任务
        val jobConf = new JobConf(configuration)
        jobConf.setOutputFormat(classOf[TableOutputFormat]) // 指定输出类型
        jobConf.set(TableOutputFormat.OUTPUT_TABLE, hbaseTableName) // 指定输出到哪张表
        //**************************************************************************



        //3. 获取到字典(url,app)
        val dicMap: collection.Map[String, String] = spark.sparkContext.textFile(dic)
            .map(_.split("\t", -1)).filter(_.length >= 5)
            .map(arr => (arr(4), arr(1))).collectAsMap()

        //3.2 关键字字典
        val wordMap = spark.sparkContext.textFile(stopWords).map((_,0)).collectAsMap()



        //4. 字典广播变量
        val broadcastDic: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(dicMap)

        //4.2 关键字广播变量
        val broadcastWord: Broadcast[collection.Map[String, Int]] = spark.sparkContext.broadcast(wordMap)



        //5. 获取日志数据
        val df: DataFrame = spark.read.parquet(input)

        var sum : Int = 0
        df.filter(TagsUtils.onUserId).rdd.map(row => {
            //6.1 获取userid
            val userid: String = TagsUtils.getAnyOneUserId(row)
            //6.2 广告标签
            val adTag: List[(String, Int)] = TagsAd.makeTags(row)//List((LC12,1), (LN视频前贴片,1))
            //6.3 媒体标签
            val appTag: List[(String, Int)] = TagsApp.makeTags(row, broadcastDic)
            //6.4 渠道标签
            val platformTag: List[(String, Int)] = TagsPlatform.makeTags(row)
//            //6.5 设备标签
            val devTag: List[(String, Int)] = TagsDev.makeTags(row)
//            //6.6 关键字标签
            val keyWordTag: List[(String, Int)] = TagsKeyWord.makeTags(row, broadcastWord)
            //6.7 地域标签
            val regionTag: List[(String, Int)] = TagsRegion.makeTags(row)

            //商圈标签
            val bussnessTag = TagsBussness.makeTags(row)

            (userid,adTag ++ appTag ++ platformTag ++ devTag ++ keyWordTag ++ regionTag ++ bussnessTag)
        })//.foreach(println)
            .reduceByKey(
                (list1, list2) => (list1:::list2)
                    //TODO groupBy不是算子 此处是高阶函数 因此没有groupByKey
                    //TODO 又因上步已经按userid进行了reduceByKey，因此此处以List((LC12,1), (LN视频前贴片,1))的LC12进行groupBy
                    .groupBy(_._1)
                    //Map(K真人秀,List((K真人秀,1),(K真人秀,1)))
                    .mapValues(list => {
                        list.map(_._2).sum
                    }
//                    _.foldLeft[Int](0)(_+_._2)
                    )
                    .toList
            )//.foreach(println)
        //(AID:4d6d28f4edd7ba31,List((K真人秀,2), (K综艺娱乐,4), (APP爱奇艺,2), (ZP广东省,2), (LC09,2), (LN视频暂停悬浮,2), (CN100018,2), (D00030004,2), (ZC中山市,2), (D00010001,2), (D00020001,2)))
        //(AID:4d6d28f4edd7ba31,List((K真人秀,2), (K综艺娱乐,4), (APP爱奇艺,2), (ZP广东省,2), (LC09,2), (LN视频暂停悬浮,2), (CN100018,2), (D00030004,2), (ZC中山市,2), (D00010001,2), (D00020001,2)))
        //(AID:c18682e4e5ace430,List((K言情剧,2), (APP爱奇艺,2), (LN视频前贴片,2), (K华语剧场,2), (ZP黑龙江省,2), (CN100018,2), (D00030004,2), (D00010001,2), (LC12,2), (ZC哈尔滨市,2), (K内地剧场,2), (D00020001,2)))
        //(AID:bb56612830241a6f,List((ZC厦门市,2), (ZP福建省,2), (APP爱奇艺,2), (LC09,2), (K华语剧场,2), (LN视频暂停悬浮,2), (CN100018,2), (D00030004,2), (K年代剧,2), (K谍战剧,2), (D00010001,2), (D00020001,2)))
            .map {
                case (userId, userTags) => {
                    // 创建rowkey
                    val put = new Put(Bytes.toBytes(userId))
                    // 处理数据
                    val tags = userTags.map(t => t._1 + "," + t._2).mkString(",")
                    put.addImmutable(Bytes.toBytes("tags"), Bytes.toBytes(day), Bytes.toBytes(tags))
                    // 返回
                    (new ImmutableBytesWritable(), put)
                }
            }.saveAsHadoopDataset(jobConf)

    }

}
