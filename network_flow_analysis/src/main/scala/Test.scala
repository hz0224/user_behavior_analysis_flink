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
import scala.collection.mutable.Set
case class ItemInfo(itemId: Long, ts: Long)

//allowedLateness的使用
object Test {
  def main(args: Array[String]): Unit = {


    val a = Set[Long]()
    val b = Set[Long]()

    a.add(1)
    a.add(2)

    b.add(1)




  }
}