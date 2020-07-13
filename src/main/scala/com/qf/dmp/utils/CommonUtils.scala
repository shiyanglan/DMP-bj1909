/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: CommonUtils
 * Author: yanglan88
 * Date: 2020/6/11 15:13
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/11
 * @since 1.0.0
 */
package com.qf.dmp.utils

import java.util.Properties

import scala.collection.mutable

object CommonUtils {
    def toMap(pro:String):Map[String,String]= {

        val properties = new Properties()
        properties.load(CommonUtils.getClass.getClassLoader.getResourceAsStream(pro))

        val map = mutable.Map[String,String]()

        val value = properties.stringPropertyNames().iterator

        while(value.hasNext){
            val str = value.next()
            map.put(str,properties.getProperty(str))
        }
        map.toMap
    }

    def toInt (str:String):Int = {
        try{
            str.toInt
        }catch{
            case _:Exception => 0
        }
    }

    def toDouble (str:String):Double = {
        try{
            str.toDouble
        }catch{
            case _:Exception => 0.0
        }
    }
}
