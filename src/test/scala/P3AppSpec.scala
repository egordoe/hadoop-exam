import java.io.IOException
import java.util.{Calendar, Date}

import edoudkin.hadoop.p3.{LogEntryReducer, LogEntryWritable, LogEntryMapper}
import org.apache.hadoop.io.{ByteWritable, Text}
import org.apache.hadoop.mrunit.mapreduce.{MapReduceDriver, ReduceDriver, MapDriver}
import org.apache.hadoop.util.GenericOptionsParser
import org.scalatest._
import collection.JavaConverters._

/**
 * Created by Egor Doudkin on 10/2/15.
 */
class P3AppSpec extends FlatSpec with Matchers {

  def mapDriver: MapDriver[Any, Text, Text, LogEntryWritable] = {
    new MapDriver(new LogEntryMapper)
  }

  def reduceDriver: ReduceDriver[Text, LogEntryWritable, Text, LogEntryWritable] = {
    new ReduceDriver(new LogEntryReducer)
  }

  def mapReduceDriver: MapReduceDriver[Any, Text, Text, LogEntryWritable, Text, LogEntryWritable] = {
    new MapReduceDriver(new LogEntryMapper, new LogEntryReducer, new LogEntryReducer)
  }

  "A mapper" should "extract an IP and size of a log entry" in {

    val logEntry = """ip1 - - [24/Apr/2011:04:06:01 -0400] "GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1" 200 40028""" +
      """ "-" "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)""""

    val output = mapDriver.withInput(new ByteWritable(0), new Text(logEntry)).run()

    output.size() should be (1)
    output.get(0).getFirst.toString should be ("ip1")
    output.get(0).getSecond.get should be ((1, 40028, -1))

  }

  "A mapper" should "fail on an invalid log entry" in {
    intercept[Exception] {
      mapDriver.withInput(new ByteWritable(0), new Text("I'm definitely not valid")).run()
    }
  }

  "A mapper" should "generate an increment of the request counters" in {

    val logEntry1 = """ip1 - - [24/Apr/2011:04:06:01 -0400] "GET /~strabal/grease/photo9/927-3.jpg HTTP/1.1" 200 40028""" +
      """ "-" "Mozilla/5.0 (compatible; YandexImages/3.0; +http://yandex.com/bots)""""
    val logEntry2 = """ip38 - - [24/Apr/2011:06:22:57 -0400] "GET /favicon.ico HTTP/1.0" 200 318 "http://www.qbik.ch/sitebar/" "SiteBar/3.2.6""""

    val driver = mapDriver
    driver.addInput(new ByteWritable(0), new Text(logEntry1))
    driver.addInput(new ByteWritable(1), new Text(logEntry2))

    driver.run()

    val counters = driver.getCounters.getGroup("REQUEST_COUNTERS")
    counters.findCounter("Mozilla/5.0").getValue should be (1)
    counters.findCounter("SiteBar/3.2.6").getValue should be (1)

  }


  "A reducer" should "correctly aggregate log entries" in {

    val logEntry1 = new LogEntryWritable(4, 100)
    val logEntry2 = new LogEntryWritable(1, 50)

    val output = reduceDriver.withInput(new Text("ip1"), List(logEntry1, logEntry2).asJava).run()

    output.size() should be (1)
    output.get(0).getFirst.toString should be ("ip1")
    output.get(0).getSecond.get should be ((5, 150, 30))

  }

}
