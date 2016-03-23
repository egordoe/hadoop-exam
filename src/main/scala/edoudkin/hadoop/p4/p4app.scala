package edoudkin.hadoop.p4

import java.lang.Iterable
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.Reader
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.hadoop.mapreduce.{Reducer, Mapper}
import collection.JavaConverters._

/**
 * This mapper maps every input key to partitionId that is calculated using Partitioner supplied and returns them in form of
 * (partitionId -> 1) where 1 is just a constant. This mapper should work in the conjunction with multiple reducers configured
 * with the same partitioner.
 */
class PartitioningMapper(val partitioner: HashPartitioner[LongWritable, Any]) extends Mapper[LongWritable, Any, IntWritable, LongWritable] {

  val const = new LongWritable(1L)
  val partitionId = new IntWritable()
  def this() = { this(new HashPartitioner)}

  override def map(key: LongWritable, value: Any, context: Mapper[LongWritable, Any, IntWritable, LongWritable]#Context): Unit = {

    partitionId.set(partitioner.getPartition(key, value, context.getNumReduceTasks))
    context.write(partitionId ,const)

  }

}

class PartitioningReducer extends Reducer[IntWritable, LongWritable, IntWritable, LongWritable] {
  val sum = new LongWritable()
  override def reduce(key: IntWritable, values: Iterable[LongWritable],
                      context: Reducer[IntWritable, LongWritable, IntWritable, LongWritable]#Context): Unit = {
    sum.set(values.asScala.map(_.get.toLong).sum)
    context.write(key, sum)
  }
}

class NumberingReducer extends Reducer[LongWritable, Text, LongWritable, Text] {
  val counter = new LongWritable(0)


  override def setup(context: Reducer[LongWritable, Text, LongWritable, Text]#Context): Unit = {

    val keyBuf = new IntWritable()
    val valBuf = new LongWritable()

    val partSizes = for (fileUri <- context.getCacheFiles if fileUri.toString.matches(""".*(part-r-\d\d\d\d\d)""")) yield {
      val reader = new Reader(context.getConfiguration, SequenceFile.Reader.file(new Path(fileUri)))
      reader.next(keyBuf, valBuf)
      keyBuf.get -> valBuf.get
    }

    val partNum = context.getTaskAttemptID.getTaskID.getId

    val startNum = partSizes.filter(_._1 < partNum).map(_._2).sum
    counter.set(startNum)

  }

  override def reduce(key: LongWritable, values: Iterable[Text],
                      context: Reducer[LongWritable, Text, LongWritable, Text]#Context): Unit = {
    values.asScala.foreach { line =>
      counter.set(counter.get + 1)
      context.write(counter, line)
    }

  }
}