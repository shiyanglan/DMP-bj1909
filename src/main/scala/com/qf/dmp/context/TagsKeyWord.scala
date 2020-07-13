/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TagsKeyWord
 * Author: yanglan88
 * Date: 2020/6/26 10:19
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
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.Row

object TagsKeyWord extends TagsTrait{
    /**
     * 打标签方法
     */
    override def makeTags(args: Any*): List[(String, Int)] = {

        //1. 定义个序列接收结果
        var list = List[(String, Int)]()

        //2. 解析参数
        val row = args(0).asInstanceOf[Row]
        val map = args(1).asInstanceOf[Broadcast[collection.Map[String, Int]]]

        //3. 取值keyword
        val kw: Array[String] = row.getAs[String]("keywords").split("\\|")

        //4. 两个条件：1.长度，2.词库
        kw.filter(
            word => word.length >= 3 && word.length <= 8 && !map.value.contains(word)
        ).foreach(
            word => list :+= ("K" + word, 1)
        )
        list
    }
}
