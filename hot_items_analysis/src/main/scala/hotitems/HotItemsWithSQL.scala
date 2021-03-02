package hotitems

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object HotItemsWithSQL {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\UserBehavior.csv")

    val userBehaviorDStream: DataStream[UserBehavior] = inputDStream.map{line=>
      val data = line.split(",")
      UserBehavior(data(0).toLong, data(1).toLong, data(2).toInt, data(3), data(4).toLong)
    }.assignAscendingTimestamps(_.ts * 1000L)

    //开窗函数目前只有blink支持,因此需要使用 blink的流式查询
    val bsSettings = EnvironmentSettings.newInstance()
      .useBlinkPlanner()    //Blink
      .inStreamingMode()  //流处理模式
      .build()

    val tableEnv = StreamTableEnvironment.create(env,bsSettings)
    val inputTable = tableEnv.fromDataStream(userBehaviorDStream,'itemId as 'item_id, 'behavior, 'ts , 'ts.rowtime as 'et)
    tableEnv.createTemporaryView("input_table",inputTable)

    //1 窗口聚合
    val windowedTable = tableEnv.sqlQuery(
      """
        |select
        |    item_id,
        |    count(item_id) as cnt,
        |    hop_end(et,interval '5' minute, interval '1' hour) as window_end
        |from
        |    input_table
        |where
        |    behavior = 'pv'
        |group by
        |    item_id,hop(et,interval '5' minute, interval '1' hour)
      """.stripMargin)

    tableEnv.createTemporaryView("windowed_table",windowedTable)

    //2 排序
    val resultTable = tableEnv.sqlQuery(
      """
        |select
        |    rk,
        |    item_id,
        |    cnt,
        |    window_end
        |from
        |    (select
        |        item_id,
        |        cnt,
        |        window_end,
        |        row_number() over(partition by window_end order by cnt desc) as rk
        |    from
        |        windowed_table
        |    ) t
        |where
        |    t.rk <= 5
      """.stripMargin)  //注意： 不要用rank做别名，rank是flinkSQL里的函数

    //打印结果
    resultTable.toRetractStream[Row].print()

    env.execute("hot_items_with_SQL")
  }
}
