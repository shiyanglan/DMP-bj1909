/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TagsAd
 * Author: yanglan88
 * Date: 2020/6/24 11:27
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

import com.qf.dmp.traits.TagsTrait
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row

object TagsAd extends TagsTrait{
    override def makeTags(args: Any*): List[(String, Int)] = {

        //1. 定义个序列接收结果
        var list = List[(String, Int)]()

        //2. 解析参数
        val row = args(0).asInstanceOf[Row]

        //3. 获取广告类型和名称
        val adType = row.getAs[Int]("adspacetype")
        val adName = row.getAs[String]("adspacetypename")

        //4. 处理补0
        adType match {
            case v if v > 9 => list:+=("LC"+v,1)
            case v if v > 0 && v <= 9 => list:+=("LC0"+v,1)
        }

        //5. 拼凑标签
        if (StringUtils.isNoneBlank(adName)) {
            list:+=("LN"+ adName, 1)
        }
        list
    }
}
