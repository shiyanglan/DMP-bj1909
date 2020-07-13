/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: AppRpt
 * Author: yanglan88
 * Date: 2020/6/25 19:35
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/25
 * @since 1.0.0
 */
package com.qf.dmp.app
import java.util.Properties

import cn.qphone.dmp.utils.RtbUtils
import com.qf.dmp.traits.Logger_Trait
import com.qf.dmp.utils.Spark_utils
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast

/**
 * 媒体分布
 */
object AppRpt extends Logger_Trait{
    def main(args: Array[String]): Unit = {

        val SPARK_PROPERTIES:String = "spark.properties"

        //1. 控制读取和存储的参数
        if(args == null || args.length != 3) {
            println("Usage : <input> <output> <dic>")
            System.exit(-1)}
        val Array(input, output, dic) = args

        //2. 获取到入口并配置序列化以及压缩方式
        val properties = new Properties()
        properties.load(AppRpt.getClass.getClassLoader.getResourceAsStream(SPARK_PROPERTIES))
        val spark = Spark_utils.getLocalSparkSession(AppRpt.getClass.getSimpleName)
        spark.sqlContext.setConf(properties)

        //3. 获取到字典(url,app)
        //cn.net.inch.android   乐自游
        val dicMap = spark.sparkContext.textFile(dic)
            .map(_.split("\t", -1)).filter(_.length >= 5)
            .map(arr => (arr(4), arr(1))).collectAsMap()

        //4. 广播变量
        val broadcastDic: Broadcast[collection.Map[String, String]] = spark.sparkContext.broadcast(dicMap)

        //5. 获取日志数据
        val df = spark.read.parquet(input)

        //6. 处理业务
        df.rdd.map(row => {

            //6.1 获取到appname
            var appName = row.getAs[String]("appname")
            //6.2 校验
            if(StringUtils.isNoneBlank(appName)) {
                appName = broadcastDic.value.getOrElse(row.getAs[String]("appid"), "其他")
            }

            //6.3 获取字段
            val requestmode:Int = row.getAs[Int]("requestmode")
            val processnode = row.getAs[Int]("processnode")
            val iseffective = row.getAs[Int]("iseffective")
            val isbilling = row.getAs[Int]("isbilling")
            val isbid = row.getAs[Int]("isbid")
            val iswin = row.getAs[Int]("iswin")
            val adorderid = row.getAs[Int]("iswin")
            val winprice = row.getAs[Double]("winprice")
            val adpayment = row.getAs[Double]("adpayment")

            //6.4 调用业务方法
            val reqList = RtbUtils.requestAd(requestmode, processnode)
            val adList = RtbUtils.adPrice(iseffective, isbilling, isbid, iswin, adorderid, winprice, adpayment)
            val clickList = RtbUtils.shows(requestmode, iseffective)
            (appName, reqList ++ adList ++ clickList)
        }).reduceByKey((list1, list2) => list1.zip(list2).map(t => t._1 + t._2)).foreach(println)

        //7. 释放资源
        Spark_utils.stop(spark)
    }
}
