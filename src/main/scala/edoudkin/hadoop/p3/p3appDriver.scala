package edoudkin.hadoop.p3

import org.apache.hadoop.conf.{Configured, Configuration}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile.CompressionType
import org.apache.hadoop.io.compress.SnappyCodec
import org.apache.hadoop.io.{SequenceFile, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, FileOutputFormat}
import org.apache.hadoop.util.{ToolRunner, Tool}

/**
 * Created by Egor Doudkin on 10/3/15.
 */
object P3AppDriver extends App {
  ToolRunner.run(new Configuration(), new P3AppDriver, args)
}

class P3AppDriver extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val conf = getConf
    //conf.set("mapreduce.output.textoutputformat.separator", ",")

    if (args.length < 2) {
      System.err.println("Usage: 'logentry <in> <out>' OR\n 'logentry -text <in>'")
      System.exit(2)
    }

    conf.setBoolean(FileOutputFormat.COMPRESS, true)
    conf.set(FileOutputFormat.COMPRESS_TYPE, CompressionType.BLOCK.toString)
    conf.set(FileOutputFormat.COMPRESS_CODEC, classOf[SnappyCodec].getName)

    if (args(0) == "-text") {

      new SeqFileReader(new Path(args(1)), conf).print()
      0

    } else {
      val job = Job.getInstance(conf, "LogEntry Job")

      FileInputFormat.addInputPath(job, new Path(args(0)))
      FileOutputFormat.setOutputPath(job, new Path(args(1)))

      job.setJarByClass(classOf[P3AppDriver])
      job.setMapperClass(classOf[LogEntryMapper])
      job.setCombinerClass(classOf[LogEntryReducer])
      job.setReducerClass(classOf[LogEntryReducer])

      job.setOutputKeyClass(classOf[Text])
      job.setOutputValueClass(classOf[LogEntryWritable])

      job.setOutputFormatClass(classOf[SequenceFileOutputFormat[Text, LogEntryWritable]])

      val res = if (job.waitForCompletion(true)) 1 else 0
      println(job.getCounters)
      res
    }
  }
}

class SeqFileReader(val filePath: Path, val conf: Configuration) {

  val reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(filePath))

  val keyBuf = new Text()
  val valBuf = new LogEntryWritable()

  def print() = {
    while(reader.next(keyBuf, valBuf)) {
      println(s"${keyBuf.toString} -> (avg=${valBuf.get._3}, total=${valBuf.get._2})")
    }
  }

}

