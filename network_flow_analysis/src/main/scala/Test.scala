import java.text.SimpleDateFormat

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ArrayBuffer

case class ItemInfo(itemId: Long, ts: Long)

//allowedLateness的使用
object Test {
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val inputDStream = env.socketTextStream("39.105.49.35",9999)
    val ItemInfoDStream = inputDStream.map{line=>
        val data = line.split(",")
        val ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(data(1)).getTime
        ItemInfo(data(0).toLong, ts)
    }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ItemInfo](Time.seconds(1)) {
      override def extractTimestamp(t: ItemInfo): Long = t.ts
    })
    val lateTag = new OutputTag[ItemInfo]("late")
    val resultDStream = ItemInfoDStream
                          .keyBy(_.itemId)
                          .timeWindow(Time.seconds(5))
                          .allowedLateness(Time.seconds(3)) //允许窗口延迟3秒关闭，即当水位 >= windowEnd + 3 时才会触发窗口的关闭.
                          .sideOutputLateData(lateTag)  //迟到数据放入侧输出流
                          .sum(0)

    //侧输出流数据
    resultDStream.getSideOutput(lateTag).print("late")

    resultDStream.print("stat")
    env.execute("test")
  }
}