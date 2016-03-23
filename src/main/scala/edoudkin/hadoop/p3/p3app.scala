package edoudkin.hadoop.p3

import java.io.{DataOutput, DataInput}
import java.lang.Iterable

import org.apache.hadoop.io.{DoubleWritable, Writable, LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper
import org.apache.hadoop.mapreduce.Reducer
import scala.collection.JavaConverters._
import scala.util.Try

/**
 * Created by Egor Doudkin on 10/2/15.
 */

class LogEntryMapper extends Mapper[Any, Text, Text, LogEntryWritable] {
  val ipText = new Text()
  val logEntry = new LogEntryWritable()

  val regex = """([^ ]*) - - (\[.*\]) "([^"]*)" (\d\d\d) (\d+|-) "[^"]*" "([^"]*)"""".r("ip", "date", "url", "code", "size", "client")

  override def map(key: Any, value: Text,
                   context: Mapper[Any, Text, Text, LogEntryWritable]#Context): Unit = {

    regex.findPrefixMatchOf(value.toString).map{ (m) =>
      val size = Try(m.group("size").toLong).getOrElse(0L)
      m.group("ip") -> (size, m.group("client"))
  } match {
      case None =>

        // We're gonna stop any further processing as an invalid line happens... Or we could just log an error and proceed.
        // It depends on how harmful is it to have some of the lines unprocessed.
        throw new RuntimeException(s"An invalid input: $value")

      case Some((ip, (size, client))) =>

        client.split(" ").find(_ != "(compatible;").foreach { client =>
          context.getCounter("REQUEST_COUNTERS", client).increment(1L)
        }

        ipText.set(ip)
        logEntry.set(countVal = 1L, totalSizeVal = size)
        context.write(ipText, logEntry)

    }

  }
}

class LogEntryReducer extends Reducer[Text, LogEntryWritable, Text, LogEntryWritable] {
  val ipText = new Text()
  val logEntry = new LogEntryWritable()

  override def reduce(key: Text, values: Iterable[LogEntryWritable],
                      context: Reducer[Text, LogEntryWritable, Text, LogEntryWritable]#Context): Unit = {

    values.asScala.foldLeft(0L -> 0L) { (vals, logEntry) =>
      (vals._1 + logEntry.get._1) -> (vals._2 + logEntry.get._2)
    } match {
      case (totalCount, totalSize) =>
        logEntry.set(totalCount, totalSize, (totalSize * 1d)/totalCount)
    }

    context.write(key, logEntry)

  }
}

/**
 * This Writable comprises of 3 fields - a number of log entries, a total size of all the entries (in bytes) and an average
 * value (total/"number of entries").
 */
class LogEntryWritable extends Writable {

  private val count = new LongWritable()
  private val totalSize = new LongWritable()
  private val avgSize = new DoubleWritable()
  def this(countVal: Long, totalSizeVal: Long) = {this(); this.set(countVal, totalSizeVal)}

  override def write(out: DataOutput): Unit = {count.write(out); totalSize.write(out); avgSize.write(out)}
  override def readFields(in: DataInput): Unit = {count.readFields(in); totalSize.readFields(in); avgSize.readFields(in)}

  def set(countVal: Long, totalSizeVal: Long, avgSizeVal: Double = -1): Unit = {count.set(countVal); totalSize.set(totalSizeVal); avgSize.set(avgSizeVal)}
  def get: (Long, Long, Double) = (count.get(), totalSize.get(), avgSize.get())

  def canEqual(other: Any): Boolean = other.isInstanceOf[LogEntryWritable]
  override def equals(other: Any): Boolean = other match {
    case that: LogEntryWritable =>
      (that canEqual this) &&
        count == that.count &&
        totalSize == that.totalSize
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(count, totalSize)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }
}