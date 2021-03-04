import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//每隔 5 秒，输出最近 10 分钟内访问量最多的前 N 个 URL

//输入数据
case class NetWorkEvent(ip: String, userId: String, ts: Long, method: String, url: String)
//结果数据
case class PageViewCount(page: String, windowEnd: Long, count: Long)

object NetworkFlow {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    val inputDStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\apache.log")
    //83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
    val netWorkEventDStream: DataStream[NetWorkEvent] = inputDStream.map{line=>
        val data = line.split(" ")
        val format = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = format.parse(data(3)).getTime
        data(0)
        NetWorkEvent(data(0), data(1), ts, data(5),data(6))
    }.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[NetWorkEvent](Time.seconds(1)) {
      override def extractTimestamp(t: NetWorkEvent): Long = t.ts
    })

    /**
      * 迟到数据3步走：
      *   1 设置watermark延迟
      *   2 allowedLateness方法延迟窗口关闭
      *   3 迟到数据放入侧输出流.
      */
    //迟到数据标签
    val lateTag = new OutputTag[NetWorkEvent]("late")
    //开窗
    val aggDStream = netWorkEventDStream.filter(netWorkEvent => "GET".equals(netWorkEvent.method))
                                                  .keyBy(_.url)
                                                  .timeWindow(Time.minutes(10), Time.seconds(5))
                                                  .allowedLateness(Time.minutes(1))   //延迟窗口关闭
                                                  .sideOutputLateData(lateTag)  //迟到数据放入侧输出流
                                                  .aggregate(new PageCountAgg(),new PageViewCountResult)
    //迟到数据
    val lateDStream = aggDStream.getSideOutput(lateTag)
    val pageViewCountResultDStream = aggDStream.keyBy(_.windowEnd).process(new pageViewKeyedProcess(5))
    pageViewCountResultDStream.print()
    env.execute("network_flow")
  }
}

//预聚合函数 AggregateFunction<IN, ACC, OUT>
class PageCountAgg() extends AggregateFunction[NetWorkEvent,Long, Long]{
  override def add(value: NetWorkEvent, accumulator: Long): Long = accumulator + 1

  override def createAccumulator(): Long = 0L

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//全窗口函数 WindowFunction[IN, OUT, KEY, W <: Window]
class PageViewCountResult extends WindowFunction[Long, PageViewCount, String, TimeWindow]{
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[PageViewCount]): Unit = {
    out.collect(PageViewCount(key, window.getEnd, input.iterator.next()))
  }
}

//KeyedProcessFunction<K, I, O>，由于是按照windowEnd分组，因此每个窗口都会初始化一次pageViewKeyedProcess
class pageViewKeyedProcess(topN: Int) extends KeyedProcessFunction[Long,PageViewCount, String]{

  lazy val allPageCountState: MapState[String,Long] = getRuntimeContext.getMapState(
    new MapStateDescriptor[String,Long]("page_view_count_state",classOf[String],classOf[Long])
  )

  //processElement方法不进行输出，只保存数据到状态里，放到onTimer方法里输出
  override def processElement(i: PageViewCount, context: KeyedProcessFunction[Long, PageViewCount, String]#Context, collector: Collector[String]): Unit = {
      allPageCountState.put(i.page,i.count) //因为是map类型，因此同一个page，迟到数据计算的结果会把之前计算的结果覆盖掉。
      //注册定时器
      context.timerService().registerEventTimeTimer(i.windowEnd + 1)
      //这个定时器触发时，代表这个窗口已经彻底关闭，不会再有新的数据进来
      context.timerService().registerEventTimeTimer(i.windowEnd + 60000L)   // windowEnd + allowedLateness方法设置的延迟.
  }

  /**
    * onTimer方法调用时有两种情况：
    *   1 (i.windowEnd + 1)定时器触发的，可能是窗口第一次计算触发，也可能是该窗口的迟到数据注册后触发的.
    *   2 (i.windowEnd + 60000L)定时器触发的，说明该窗口已经彻底关闭
    */
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, PageViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //如果该窗口已经彻底关闭，清空状态，同时return。相当于这个MapState的生命周期是从第一次窗口计算到窗口彻底关闭。
    if(timestamp == ctx.getCurrentKey + 60000L){
        allPageCountState.clear()
        return  //该窗口已经彻底关闭，不需要做任何处理了.
    }
    //能执行到这，说明一定是 (i.windowEnd + 1)定时器触发的onTimer方法。

    //1 拷贝状态里的数据
    val listBuffer = new ListBuffer[PageViewCount]()
    val iter = allPageCountState.entries().iterator()

    while (iter.hasNext){
        val entry = iter.next()
        listBuffer += PageViewCount(entry.getKey, ctx.getCurrentKey,entry.getValue)
    }
    //2 排序
    val resultList = listBuffer.sortWith(_.count > _ .count).take(topN)
    //3 打印输出
    val sb = new StringBuilder

    sb.append("================== "+ new java.sql.Timestamp (timestamp - 1 ) +" ========================\n\n")
    for(i <- 0 until resultList.size){
      sb.append("No: " + (i + 1)).append("\t")
        .append("page: ").append(resultList(i).page).append("\t")
        .append("count: ").append(resultList(i).count).append("\t")
        .append("windowEnd: " + resultList(i).windowEnd)
        .append("\n\n")
    }
    sb.append("============================================================\n\n")
    out.collect(sb.toString())
    Thread.sleep(1000) // 便于观察打印结果
  }
}

/*
* 文字分析：
* 第一次触发窗口聚合函数计算得到结果：
page    windowEnd    count
page1   w1           5
page2   w1           4
page3   w1           8
然后每条数据经过processElement方法后会把这些数据存储 mapState里.
然后水位上涨触发定时器(windowEnd + 1 )计算调用onTimer方法进行排序输出.

接着又来3条该窗口的迟到数据，那么每条数据都会都会触发窗口的聚合计算
page1   w1           6
page1   w1           7
page3   w1           9
然后每条数据经过processElement方法后又会注册定时器同时把这些数据存储 mapState里（由于是map，因此新的计算结果会覆盖旧的，同时会保留没有变化的数据结果）
然后水位接着上涨又会触发定时器（windowEnd + 1 ）计算 调用onTimer方法进行排序输出。

等到 （windowEnd + 设置的窗口延迟关闭）定时器触发时，代表这个窗口已经彻底关闭，不会再有新的数据进来，此时清空mapState状态。
定义一个ts=5的定时器，当watermark >= 5时会触发，然后定时器会销毁。
接着如果再定义一个ts=5的定时器，这个时候watermark肯定已经大于5了，那么只要这个watermark再上涨一次，这个ts=5的定时器仍会触发一次。
可见，当使用了allowedLateness方法后，窗口的计算就会变得复杂很多，需要具体情况，具体分析。
根本原因就是使用了allowedLateness后，迟到数据会和之前窗口计算的结果进行再一次计算，同时会输出这一次的计算结果，因为之前窗口计算已经输出过一次了，所以
同一个key会输出多个计算结果，最后那条数据才是最新最后的计算结果。
*
* */