package org.apache.flink

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment

object Test {
  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val tEnv = StreamTableEnvironment.create(env)


    // 定义数据源表
 /*  tEnv.executeSql("""
                      |CREATE TABLE site_inverter (
                      |    site_id BIGINT,
                      |    inverter_id    BIGINT,
                      |    start_time   TIMESTAMP(3),
                      |    PRIMARY KEY(site_id, inverter_id) NOT ENFORCED
                      |) WITH (
                      |'connector' = 'datagen',
                      |'number-of-rows' = '5',
                      |'fields.site_id.min' = '1',
                      |'fields.site_id.max' = '20',
                      |'fields.inverter_id.min' = '1001',
                      |'fields.inverter_id.max' = '1100'
                      |)
                      |""".stripMargin)*/

    //tEnv.sqlQuery("select order_number,price from orders").execute().print();


    // 定义 redis 表
    tEnv.executeSql(
      """
        |create table redis_sink (
        |site_id STRING,
        |inverter_id STRING,
        |start_time STRING,
        |PRIMARY KEY(site_id, inverter_id) NOT ENFORCED
        |) WITH (
        |'connector' = 'redis',
        |'mode' = 'single',
        |'single.host' = 'test1',
        |'single.port' = '6379',
        |'password' = '6U2sRNV6Jexex6R#',
        |'command' = 'hgetall',
        |'key' = 'site_inverter:site_id:20:inverter_id:1059,site_inverter:site_id:17:inverter_id:1082',
        |'primary.key'='site_id,inverter_id'
        |)
        |""".stripMargin)




    // 执行插入 SQL
    /*tEnv.executeSql(
      """
        |insert into redis_sink
        |select
        |cast(site_id as string),
        |cast(inverter_id as string),
        |cast(start_time as string)
        |from site_inverter
        |""".stripMargin)*/


    tEnv.sqlQuery("select * from redis_sink ").execute().print()

  }
}
