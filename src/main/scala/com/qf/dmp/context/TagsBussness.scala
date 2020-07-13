/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: TagsBussness
 * Author: yanglan88
 * Date: 2020/7/12 14:33
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/7/12
 * @since 1.0.0
 */
package com.qf.dmp.context

import ch.hsr.geohash.GeoHash
import com.qf.dmp.traits.TagsTrait
import com.qf.dmp.utils.{AmapUtils, JedisUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.Row
import redis.clients.jedis.Jedis

object TagsBussness extends TagsTrait{
    /**
     * 从redis缓存库中查询商圈是否存在
     */
    def queryBussnessWithRedis(geoHash: String) = {
        //1. 获取jedis
        val jedis: Jedis = JedisUtils.getJedisPool.getResource
        //2. 获取数据
        val str: String = jedis.get(geoHash)
        //3. 释放
        JedisUtils.release(jedis)
        //4. 返回
        str
    }

    /**
     * 将商圈信息缓存到redis
     */
    def saveBussnessWithRedis(geoHash: String, bussness: String) = {
        val jedis = JedisUtils.getJedisPool.getResource
        jedis.set(geoHash, bussness)
        JedisUtils.release(jedis)
    }

    /**
     * 通过经纬度获取到商圈信息
     */
    def getBussness(long: Double, lat: Double) = {
        //1. 将经纬度转换程GeoHash编码
        val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(lat, long, 8)

        //2. 先去缓存库查询，没有再到第三方开放平台去查询
        var bussness: String = queryBussnessWithRedis(geoHash)

        //2.1 判断是否为空
        if (bussness == null || bussness.length == 0) {

            //2.2 从第三方去查询
            bussness = AmapUtils.getBussnessFromAMap(long, lat)

            //3. 将商圈信息保存
            if (bussness != null && bussness.length > 0) {
                //3.1 丢进redis缓存
                saveBussnessWithRedis(geoHash, bussness)
                //TODO WX4ER当作key，把该区域的餐馆信息当作value
                //TODO (WX4ER,(望京，酒仙桥，大山子))
            }

        }
        bussness
    }

    override def makeTags(args: Any*): List[(String, Int)] = {

        val list: List[(String, Int)] = List[(String, Int)]()

        val row: Row = args(0).asInstanceOf[Row]

        //3. 获取经纬度，要考虑经纬度是否正确
        if(row.getAs[String]("long").toDouble >= 73
            && row.getAs[String]("long").toDouble  <= 135
            && row.getAs[String]("lat").toDouble  >= 3
            && row.getAs[String]("lat").toDouble  <= 53) {
            //3.1 获取到经纬度
            val long = row.getAs[String]("long")
            val lat = row.getAs[String]("lat")

            //3.2 通过经纬度获取到商圈信息
            val bussness: String = getBussness(long.toDouble, lat.toDouble)
            if(StringUtils.isNoneBlank(bussness)){
                val strArr = bussness.split(",")
                strArr.foreach(
                    t => list :+ (t,1)
                )
            }
        }

        list.distinct

    }
}
