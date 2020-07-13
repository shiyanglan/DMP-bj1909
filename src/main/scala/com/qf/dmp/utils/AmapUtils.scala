package com.qf.dmp.utils

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

object AmapUtils {

    /**
     * 通过经纬度查询商圈信息
     */
    def getBussnessFromAMap(long: Double, lat: Double): String = {
        //1. 拼凑字符串：https://restapi.amap.com/v3/geocode/regeo?key=5a0ba59858047ad724a1c98ee9cd08df&location=long,lat
        val location = long + "," + lat
        val urlStr = "https://restapi.amap.com/v3/geocode/regeo?key=5a0ba59858047ad724a1c98ee9cd08df&location=" + location

        //2. 发送请求
        val str: String = HttpUtils.get(urlStr)

        //3. 解析json
        val jsonObject: JSONObject = JSON.parseObject(str)
        val status: Int = jsonObject.getIntValue("status")

        //4. 校验是否成功
        if (status == 0) return ""

        //5. 解析商圈信息
        val regeocode: JSONObject = jsonObject.getJSONObject("regeocode")
        if (regeocode == null) return null

        val addressComponent: JSONObject = regeocode.getJSONObject("addressComponent")
        if (addressComponent == null) return null

        val businessAreas: JSONArray = addressComponent.getJSONArray("businessAreas")
        if (businessAreas == null) return null

        //6. 创建集合
        val result = ListBuffer[String]()
        //7. 循环遍历
        for(item <- businessAreas.toArray()) {
            //7.1 类型判断
            if (item.isInstanceOf[JSONObject]) {
                //7.2 转换类型
                val json = item.asInstanceOf[JSONObject]
                //7.3 获取名称
                val name = json.getString("name")
                //7.4 添加商圈名称到集合
                result.append(name)
            }
        }
        result.mkString(",") // 望京，酒仙桥，大山子
    }
}
