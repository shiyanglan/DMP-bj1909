/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TagsApp
 * Author: yanglan88
 * Date: 2020/6/25 22:32
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/6/25
 * @since 1.0.0
 */
package com.qf.dmp.context

import com.qf.dmp.traits.TagsTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsApp extends TagsTrait{
    override def makeTags(args: Any*): List[(String, Int)] = {
        //1. 获取结果序列
        var list = List[(String, Int)]()

        //2. 解析参数
        val row: Row = args(0).asInstanceOf[Row]
        val map: Broadcast[collection.Map[String, String]] = args(1).asInstanceOf[Broadcast[collection.Map[String, String]]]

        //3. 获取appname、appid
        val appName = row.getAs[String]("appname")
        val appId = row.getAs[String]("appid")

        //4. 如果存在就直接获取，不存在设置为其他
        if(StringUtils.isNoneBlank(appName)) {
            list:+=("APP" + appName, 1)
        }else {
            list:+=("APP" + map.value.getOrElse(appId, "其他"), 1)
        }
        list
    }
}
