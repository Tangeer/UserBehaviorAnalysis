package com.longrise

/**
 * 热门流量的实时统计
 */

import java.sql.Timestamp
import java.text.SimpleDateFormat

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


// 数据输入格式
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
// 输出数据格式
case class UrlViewCount(url: String, windowEnd: Long, count: Long)

object NetworkTraffic {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.readTextFile("F:\\Project\\UserBehaviorAnalysis\\NetworkTracfficAnalysis\\src\\main\\resources\\apache.log")
      .map(line => {
        val linearray: Array[String] = line.split(" ")
        // 定义时间转换模板将时间转成时间戳
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val timestamp: Long = simpleDateFormat.parse(linearray(3)).getTime
        ApacheLogEvent(linearray(0), linearray(1), timestamp, linearray(5), linearray(6))
      })
        //乱序数据处理，创建时间戳和水位
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
        override def extractTimestamp(t: ApacheLogEvent): Long = {
          t.eventTime
        }
      })
      .filter(_.method == "GET")
      .keyBy(_.url)
      .timeWindow(Time.minutes(1), Time.seconds(5))
      .aggregate(new CountAgg(), new WindowResuleFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotUrls(5))
      .print()
    env.execute("Network Traffic Analysis Job")
  }

  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long]{
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResuleFunction extends WindowFunction[Long, UrlViewCount, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val url: String = key
      val count: Long = input.iterator.next()
      out.collect(UrlViewCount(url, window.getEnd, count))
    }
  }

  // 自定义process function, 统计访问量最大的url, 排序输出
  class TopNHotUrls(topSize: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] {
    // 直接定义状态变量， 懒加载
    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState",classOf[UrlViewCount]))
    override def processElement(i: UrlViewCount, context: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
      // 把每条数据保存到状态中
      urlState.add(i)
      // 注册一个定时器，windowEnd + 10秒时触发
      context.timerService().registerEventTimeTimer(i.windowEnd + 10 * 1000)
    }
    // 实现ontimer
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      // 从状态中获取所有的Url访问量
      val allUrlViews: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (urlView <- urlState.get()){
        allUrlViews += urlView
      }
      // 清空state
      urlState.clear()
      // 按照访问量排序输出
      val sortedUrlViews: ListBuffer[UrlViewCount] = allUrlViews.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
      // 将排名信息格式化成String ，便于打印
      val result = new StringBuilder
      result.append("=============================================\n")
      result.append("时间： ").append(new Timestamp(timestamp - 10*1000)).append("\n")

      for (i <- sortedUrlViews.indices) {
        val currentUrlView: UrlViewCount = sortedUrlViews(i)
        // e.g.  No1：  URL=/blog/tags/firefox?flav=rss20  流量=55
        result.append("No").append(i+1).append(":")
          .append("  URL=").append(currentUrlView.url)
          .append("  流量=").append(currentUrlView.count).append("\n")
      }
      result.append("=============================================\n\n")
      Thread.sleep(500)
      out.collect(result.toString())

    }
  }

}
