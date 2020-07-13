/**
 * Copyright (C), 2015-2020, XXX有限公司
 * FileName: Garphx
 * Author: yanglan88
 * Date: 2020/7/12 20:20
 * History:
 * <author> <time> <version>
 * 作者姓名 修改时间 版本号 描述
 */


/**
 * @author yanglan88
 * @create 2020/7/12
 * @since 1.0.0
 */
package com.qf.dmp.garphx

import org.apache.spark.graphx.{Edge, Graph, VertexId, VertexRDD}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Garphx {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession.builder()
            .master("local[*]")
            .appName("Garphx")
            .getOrCreate()

        //构建点
        val vertexRDD: RDD[(Long, (String, Int))] = spark.sparkContext.makeRDD(Seq(
            (1L, ("梅西", 34)),
            (2L, ("内马尔", 28)),
            (6L, ("C罗", 35)),
            (9L, ("库蒂尼奥", 37)),
            (133L, ("法尔考", 35)),
            (16L, ("詹姆斯", 35)),
            (44L, ("姚明", 36)),
            (21L, ("库里", 30)),
            (138L, ("奥尼尔", 43)),
            (5L, ("马云", 55)),
            (7L, ("马化腾", 46)),
            (158L, ("任正非", 60))
        ))

        //构建边
        val edgeRDD: RDD[Edge[Int]] = spark.sparkContext.makeRDD(Seq(
            Edge(1L, 133L, 0),
            Edge(2L, 133L, 0),
            Edge(6L, 133L, 0),
            Edge(9L, 133L, 0),
            Edge(6L, 138L, 0),
            Edge(16L, 138L, 0),
            Edge(44L, 138L, 0),
            Edge(21L, 138L, 0),
            Edge(7L, 158L, 0),
            Edge(5L, 158L, 0)
        ))

        //2. 使用图计算，找到他们顶点
        //2.1 获取到一个图对象
        val graph: Graph[(String, Int), Int] = Graph(vertexRDD, edgeRDD)
        //2.2 找到图中的顶点id(最小的点)
        val vertices: VertexRDD[VertexId] = graph.connectedComponents().vertices
        /* 每个点,顶点
         *  (158,5)
            (1,1)
            (138,1)
            (7,5)
            (16,1)
            (44,1)
            (21,1)
            (133,1)
            (5,5)
            (2,1)
            (9,1)
            (6,1)
         */
        //        vertices.foreach(println)

        //3. 获取具体的值
        val value: RDD[(VertexId, (VertexId, (String, Int)))] = vertices.join(vertexRDD)
        value.map {
            case(userid, (cmid, (name, age))) => (cmid, List((name, age)))
        }.reduceByKey(_ ++ _).foreach(println)

    }

}
