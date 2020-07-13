/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TagsDev
 * Author: yanglan88
 * Date: 2020/6/26 10:20
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/26
 * @since 1.0.0
 */
package com.qf.dmp.context

import com.qf.dmp.traits.TagsTrait
import org.apache.spark.sql.Row

object TagsDev extends TagsTrait{
    /**
     * 打标签方法
     */
    override def makeTags(args: Any*): List[(String, Int)] = {

        //1. 定义个序列接收结果
        var list = List[(String, Int)]()

        //2. 解析参数
        val row = args(0).asInstanceOf[Row]

        //3. 获取设备类型
        val client = row.getAs[Int]("client")
        client match {
            case 1 => list :+= ("D00010001", 1)
            case 2 => list :+= ("D00010002", 1)
            case 3 => list :+= ("D00010003", 1)
            case _ => list :+= ("D00010004", 1)
        }

        //4. 设备联网方式
        val network = row.getAs[String]("networkmannername")
        network match {
            case "Wifi" => list :+= ("D00020001", 1)
            case "4G" => list :+= ("D00020002", 1)
            case "3G" => list :+= ("D00020003", 1)
            case "2G" => list :+= ("D00020004", 1)
            case _ => list :+= ("D00020005", 1)
        }

        //5. 设备运营商方式
        val ispname = row.getAs[String]("ispname")
        ispname match {
            case "移动" => list :+= ("D00030001", 1)
            case "联通" => list :+= ("D00030002", 1)
            case "电信" => list :+= ("D00030003", 1)
            case _ => list :+= ("D00030004", 1)
        }

        list
    }
}
