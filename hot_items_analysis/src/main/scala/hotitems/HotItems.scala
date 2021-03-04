package hotitems

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//输入数据
case class UserBehavior(userId: Long, itemId: Long, categoryId: Int, behavior: String, ts: Long)
//窗口聚合结果
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)

//每5分钟统计一次最近1个小时热度最高的商品(浏览量越高代表热度越高)
object HotItems {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\UserBehavior.csv")

    val userBehaviorDStream: DataStream[UserBehavior] = inputDStream.map{line=>
      val data = line.split(",")
      UserBehavior(data(0).toLong, data(1).toLong, data(2).toInt, data(3), data(4).toLong)
    }.assignAscendingTimestamps(_.ts * 1000L)

    val windowedDStream: DataStream[ItemViewCount] = userBehaviorDStream.filter(userBehavior=>"pv".equals(userBehavior.behavior))
                       .keyBy(_.itemId)
                       .timeWindow(Time.hours(1),Time.minutes(5))
                       .aggregate(new CountAgg,new ItemViewCountResult())

    //这里可以调用process方法传一个全窗口函数 processWindowFunction实现聚合并取得窗口的信息
    //也可以使用aggregate方法传一个增量聚合函数和一个全窗口函数来实现同样的效果。
    /**
      * windowedDStream里的数据：
      *  itemId  window count
      *   1       w1    2
      *   2       w1    3
      *   3       w2    4
      * 即每个itemId出来一条数据，并且注意这些数据并不是封装在一个DataStream里的，可以理解为是一个DataStream里有一条数据。
      * 这也是Flink和SparkStream的不同之处，因此我们是不能将开窗计算得到的结果直接写入到存储系统里的，因为他们是一条数据封装在一个DataStream里的，而不是
      * 一个DataStream里封装了开窗后的多条数据，由于DataStream与DataStream是没有状态的，因此这些数据是无界的，是无法区分的。
      * 下面我们想对同一个窗口内的数据按照 count进行排序，此时就需要按照 windowEnd进行分组，然后调用process方法将同一个组内的数据使用状态保存起来
      * 这样就实现了分别保存每个窗口的数据，然后注册定时器，在窗口结束+1毫秒时触发排序输出，之所以加1毫秒是为了保证窗口已经关闭。
      *
      * 这个需求关键是需要对开窗后的数据排序(开窗得到结果后再统计)，那么这就需要将同一个窗口内的数据收集起来，但Flink又和Spark不同，数据是一条一条来的，不是批处理
      * 那么就需要将开窗计算得到的一条一条数据收集起来，此时就只能使用状态来保存同一个窗口计算得到的结果，然后定义触发器触发计算。
      * 注意：是保存开窗计算得到的结果，是对已经开窗后的数据进行收集保存，而不是开窗前保存。
      */
    val resultDStream: DataStream[String] = windowedDStream.keyBy(_.windowEnd).process(new MyKeyedProcessFunction(5))
    resultDStream.print()
    env.execute("hot_items")
  }
}

//自定义预聚合函数  AggregateFunction<IN, ACC, OUT>
class CountAgg() extends AggregateFunction[UserBehavior,Long,Long]{
  //每来一条数据 accumulator 加1
  override def add(value: UserBehavior, accumulator: Long) = accumulator + 1
  //初始化 accumulator
  override def createAccumulator() = 0L
  //返回结果
  override def getResult(accumulator: Long) = accumulator
  //两个accumulator合并，其实这个方法是在sessionWindow里两个窗口合并时用的。
  override def merge(a: Long, b: Long) = a + b
}

//自定义窗口函数 在 org.apache.flink.streaming.api.scala.function.WindowFunction 这个包下
//WindowFunction[IN, OUT, KEY, W <: Window] 这里IN的类型是预聚合函数的输出类型，OUT是整个窗口函数aggregate最终返回的类型。
class ItemViewCountResult() extends WindowFunction[Long,ItemViewCount,Long,TimeWindow]{
  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
       //Iterable里只有一条数据，就是预聚合函数得到的结果
       out.collect(ItemViewCount(key,window.getEnd,input.iterator.next()))
  }
}

//KeyedProcessFunction<K, I, O>
class MyKeyedProcessFunction(topN: Int) extends KeyedProcessFunction[Long,ItemViewCount,String]{

  //定义状态来保存同一个窗口内的所有数据
  lazy val allItemState: ListState[ItemViewCount] = getRuntimeContext.getListState(
    new ListStateDescriptor[ItemViewCount]("",classOf[ItemViewCount])
  )

  //processElement方法不进行输出，放到onTimer方法里进行输出
  override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]) = {
    //1 将窗口内的所有数据保存到状态里
    allItemState.add(i)
    //2 注册定时器(同一个组的数据 windowEnd相同，因此注册的是同一个定时器)
    context.timerService().registerEventTimeTimer(i.windowEnd + 1)
  }

  //定时器触发方法
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]) = {
    //1 listState里的数据拷贝到 ListBuffer中
    val listBuffer = ListBuffer[ItemViewCount]()
    val iter = allItemState.get().iterator()
    while (iter.hasNext){
      listBuffer += iter.next()
    }
    //2 清空状态
    allItemState.clear()
    //3 排序
    val result = listBuffer.sortWith(_.count > _.count).take(topN)
    //4 输出结果
    val sb = new StringBuilder
    sb.append("================== "+ new java.sql.Timestamp (timestamp - 1 ) +" ========================\n\n")

    for(i <- 0 until result.size){
        sb.append("No: " + (i + 1)).append("\t")
          .append("itemId: ").append(result(i).itemId).append("\t")
          .append("count: ").append(result(i).count).append("\t")
          .append("windowEnd: " + result(i).windowEnd)
          .append("\n\n")
    }
    sb.append("============================================================\n\n")
    out.collect(sb.toString())
    Thread.sleep(1000) // 便于观察打印结果
  }
}