/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Spark_utils
 * Author: yanglan88
 * Date: 2020/6/3 10:18
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/3
 * @since 1.0.0
 */
package com.qf.dmp.utils

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object Spark_utils {
    
//    def getStreamingContext(appName:String,master:String,seconds:Long) : StreamingContext = {
//        new StreamingContext(getSparkContext(appName,master),Seconds(seconds))
//    }
//
//    def getLocalStreamingContext(appName:String,seconds:Long) : StreamingContext = {
//        getStreamingContext(appName,"local[*]",seconds)
//    }
    
    
    

    def getSparkContext(appName:String,master:String) : SparkContext = {

        new SparkContext(new SparkConf().setAppName(appName).setMaster(master))
    }

    def getLocalSparkContext(appName:String) : SparkContext = {
        getSparkContext(appName,"local[*]")
    }





    def getSparkSession(appName:String,master:String) : SparkSession = {

        SparkSession.builder().appName(appName).master(master).getOrCreate()
    }

    def getLocalSparkSession(appName:String) : SparkSession = {

        getSparkSession(appName,"local[*]")
    }

    def getSparkSessionSupportHive (appName:String, master:String) : SparkSession = {

        SparkSession.builder().appName(appName).master(master).enableHiveSupport().getOrCreate()
    }

    def getLocalSparkSession(appName:String, supportHive:Boolean): SparkSession = {
        if (supportHive) getSparkSessionSupportHive(appName, "local[*]")
        else getSparkSession(appName, "local[*]")
    }





    def stop(sc:SparkContext) = {
        if (sc != null) sc.stop()
    }

    def stop(sc:SparkSession) = {
        if (sc != null) sc.stop()
    }
//    def stop (ssc : StreamingContext) = {
//        if (ssc != null) ssc.stop()
//    }

    def stop(sc:SparkContext, ss:SparkSession ) : Unit = {
        if (sc != null) sc.stop()
        if (ss != null) ss.stop()
//        if (ssc != null) ssc.stop()
    }

}
