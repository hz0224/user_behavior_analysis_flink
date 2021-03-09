import org.apache.commons.lang3.mutable.Mutable
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable
import scala.collection.mutable.Set
import scala.util.Random
case class UvCount(windowEnd: Long, count: Long)

//统计每小时的uv
object UniqueVisitor {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(10)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\UserBehavior.csv")
    val userBehaviorDStream = inputDStream.map{line=>
      val data = line.split(",")
      UserBehavior(data(0).toLong, data(1).toLong, data(2).toInt, data(3), data(4).toLong)
    }.assignAscendingTimestamps(_.ts * 1000L)

    val windowedDStream = userBehaviorDStream.filter{userBehavior=>"pv".equals(userBehavior.behavior)}
                          .map{userBehavior=>               //增加并行度
                            (Random.nextInt(10).toString, userBehavior.userId)
                          }.keyBy(_._1)
                           .timeWindow(Time.hours(1))
                           .aggregate(new UvSet(),new UvWindowFunction())
    val resultDStream = windowedDStream.keyBy(_._1).process(new UvKeyedProcessFunction())

    resultDStream.print()

    env.execute("unique_visitor")
  }
}
//AggregateFunction<IN, ACC, OUT>  用一个set维护当前分区里的userId先进行一波预去重，最后将这一个set输出
class UvSet() extends AggregateFunction[(String,Long),Set[Long],Set[Long]]{
  override def add(value: (String, Long), accumulator: Set[Long]): Set[Long] = accumulator += value._2

  override def createAccumulator(): Set[Long] = Set[Long]()

  override def getResult(accumulator: Set[Long]): Set[Long] = accumulator

  override def merge(a: Set[Long], b: Set[Long]): Set[Long] = {
    val set = Set[Long]()
    for(e <- a) set.add(e)
    for(b <- a) set.add(b)
    set
  }
}

// WindowFunction[IN, OUT, KEY, W <: Window]
class UvWindowFunction() extends WindowFunction[Set[Long],(Long,Set[Long]),String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[mutable.Set[Long]], out: Collector[(Long, mutable.Set[Long])]): Unit = {
    out.collect((window.getEnd, input.head))
  }
}
//KeyedProcessFunction<K, I, O>
class UvKeyedProcessFunction() extends KeyedProcessFunction[Long,(Long,Set[Long]),UvCount]{
  lazy val uvSetState: ValueState[Set[Long]] = getRuntimeContext.getState(
    new ValueStateDescriptor[mutable.Set[Long]]("uv_state",classOf[mutable.Set[Long]]) // 状态里存储的是一个引用类型，因此初始化后为null
  )
  override def processElement(i: (Long, mutable.Set[Long]), context: KeyedProcessFunction[Long, (Long, mutable.Set[Long]), UvCount]#Context, collector: Collector[UvCount]): Unit = {
    //第一次调用初始化Set
    if(uvSetState.value() == null){
      uvSetState.update(mutable.Set[Long]())
    }
    val set = uvSetState.value()
    //更新state,由于状态里存的是一个Set，是一个引用类型的地址，因此对这个Set进行改变也就对状态里的Set进行改变，所以无需使用update方法进行状态更新
    for(userId <- i._2) set.add(userId)
    //注册定时器
    context.timerService().registerEventTimeTimer(context.getCurrentKey + 1)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, (Long, mutable.Set[Long]), UvCount]#OnTimerContext, out: Collector[UvCount]): Unit = {
    out.collect(UvCount(ctx.getCurrentKey, uvSetState.value().size))
    //清空状态
    uvSetState.clear()
  }
}