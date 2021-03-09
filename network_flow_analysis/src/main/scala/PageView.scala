import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.util.Random

case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, ts: Long)
case class PvCount(windowEnd: Long, count: Long)

//统计每小时的pv
object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\UserBehavior.csv")
    val userBehaviorDStream = inputDStream.map{line=>
        val data = line.split(",")
        UserBehavior(data(0).toLong, data(1).toLong, data(2).toInt, data(3), data(4).toLong)
    }.assignAscendingTimestamps(_.ts * 1000L)

    //方式一：所有的数据分到一个分区里，这个分区里开了一个窗口(就在这一个分区里开了一个窗口，这个窗口内每组的数据调用一次聚合函数)
//    val resultDStream = userBehaviorDStream.filter{userBehavior=>"pv".equals(userBehavior.behavior)}
//                        .map{userBehavior=>("page",1L)}
//                        .keyBy(_._1)
//                        .timeWindow(Time.hours(1))
//                        .aggregate(new PageAgg(),new FullWindowFunction())
//    resultDStream.print()

    //方式二：所有的数据分到多个分区里，每个分区里都开一个窗口.
    val windowedDStream = userBehaviorDStream.filter{userBehavior=>"pv".equals(userBehavior.behavior)}
      .map{userBehavior=>
          val i = Random.nextInt(10)
          (i.toString, 1L)
      }.keyBy(_._1)
      .timeWindow(Time.hours(1))
      .aggregate(new PageAgg(),new FullWindowFunction())
    //注意：是每个分区都会开一个窗口(windEnd一样)，然后将这个窗口内所有的数据进行分组，每个分组调用一次聚合函数得到结果(每个分区的数据肯定是不同特征的)
    //因此有时候我们想要对同一个windowEnd下不同分区开窗得到的结果再聚合时就需要按照windowEnd进行keyBy

    //这样使用sum时是来一条计算一条输出一条结果,我们不想要这样的结果,我们想要一次输出聚合结果,因此还要使用定时器加状态来完成.
//    val resultDStream = windowedDStream.keyBy(_.windowEnd).sum(1)
    val resultDStream = windowedDStream.keyBy(_.windowEnd).process(new CustomKeyedProcessFunction())
    resultDStream.print()

    //统计pv时不需要考虑窗口数据量大小的问题，因为不需要对所有的数据作去重操作，因此最后不需要保存所有数据，只是简单的维护了一个递增的状态.
    env.execute("page_view")
  }
}

//AggregateFunction<IN, ACC, OUT>
class PageAgg() extends AggregateFunction[(String,Long), Long, Long]{
  override def add(value: (String, Long), accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//WindowFunction[IN, OUT, KEY, W <: Window]
class FullWindowFunction() extends WindowFunction[Long,PvCount,String,TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PvCount]): Unit = {
    out.collect(PvCount(window.getEnd, input.head))
  }
}
//KeyedProcessFunction<K, I, O>
class CustomKeyedProcessFunction() extends KeyedProcessFunction[Long, PvCount, PvCount]{
  lazy val pvCountState: ValueState[Long] = getRuntimeContext.getState(
    new ValueStateDescriptor[Long]("value_state",classOf[Long])
  )
  override def processElement(i: PvCount, context: KeyedProcessFunction[Long, PvCount, PvCount]#Context, collector: Collector[PvCount]): Unit = {
    //保存到状态
    pvCountState.update(pvCountState.value() + i.count)
    //注册定时器
    context.timerService().registerEventTimeTimer(context.getCurrentKey + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PvCount, PvCount]#OnTimerContext, out: Collector[PvCount]): Unit = {
     out.collect(PvCount(ctx.getCurrentKey, pvCountState.value()))
     //清空状态
    pvCountState.clear()
  }
}
