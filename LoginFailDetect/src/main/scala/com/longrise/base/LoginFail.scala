package com.longrise.base

/**
 * 恶意登陆实时监控
 */

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

// 定义输入的登陆事件流
case class LoginEvent(userId: Long, ip: String, eventType: String, eventTime: Long )
object LoginFail {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430842),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .filter(_.eventType == "fail")
      .keyBy(_.userId) // 按照userId做分流
      .process(new MatchFunction())
      .print()

    env.execute("Login Fail Detect Job")
  }

  // 自定义KeyedProcessFunction
  class MatchFunction() extends KeyedProcessFunction[Long, LoginEvent, LoginEvent]{
    // 定义状态变量
    lazy val loginState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loginState", classOf[LoginEvent]))
    override def processElement(i: LoginEvent, context: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#Context, collector: Collector[LoginEvent]): Unit = {
      loginState.add(i)
      // 注册定时器，设定为2秒后触发
      context.timerService().registerEventTimeTimer(i.eventTime + 2*1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginEvent]#OnTimerContext, out: Collector[LoginEvent]): Unit = {
      val allLogins: ListBuffer[LoginEvent] = ListBuffer()
      import scala.collection.JavaConversions._
      for (login <- loginState.get()){
        allLogins += login
      }
      loginState.clear()

      if (allLogins.length > 1){
        out.collect(allLogins.head)
      }
    }
  }

}
