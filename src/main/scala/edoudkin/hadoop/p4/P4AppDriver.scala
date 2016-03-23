package edoudkin.hadoop.p4

import java.util.Date

import org.apache.hadoop.conf.{Configuration, Configured}
import org.apache.hadoop.fs.{LocatedFileStatus, RemoteIterator, FileSystem, Path}
import org.apache.hadoop.io.{Text, IntWritable, LongWritable}
import org.apache.hadoop.mapreduce.{Mapper, Job}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{SequenceFileOutputFormat, TextOutputFormat, MultipleOutputs, FileOutputFormat}
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.hadoop.util.{Tool, ToolRunner}

/**
 * Created by Egor Doudkin on 10/4/15.
 */
object P4AppDriver extends App {
  ToolRunner.run(new Configuration(), new P4AppDriver, args)
}

class P4AppDriver extends Configured with Tool {
  override def run(args: Array[String]): Int = {
    val conf = getConf

    if (args.length < 3) {
      System.err.println("Usage: 'partitioning <in> <out> <number_of_reducers>'")
      System.exit(2)
    }

    val partitioningJob = createPartitioningJob(args)

    if (partitioningJob.waitForCompletion(true)) {
      val numberingJob = createNumberingJob(args, FileOutputFormat.getOutputPath(partitioningJob))
      if (numberingJob.waitForCompletion(true)) 1 else 0
    } else 0
  }

  def createPartitioningJob(args: Array[String]): Job = {
    val job = Job.getInstance(getConf, "Partitioning Job")
    val now = new Date

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1), s"out_part_${now.getTime}"))

    job.setJarByClass(classOf[P4AppDriver])
    job.setMapperClass(classOf[PartitioningMapper])
    job.setCombinerClass(classOf[PartitioningReducer])
    job.setReducerClass(classOf[PartitioningReducer])

    job.setNumReduceTasks(args(2).toInt)
    job.setPartitionerClass(classOf[HashPartitioner[Any, Any]])

    job.setOutputKeyClass(classOf[IntWritable])
    job.setOutputValueClass(classOf[LongWritable])
    job.setOutputFormatClass(classOf[SequenceFileOutputFormat[IntWritable, LongWritable]])
    job
  }


  def createNumberingJob(args: Array[String], partitionsDir: Path): Job = {
    val job = Job.getInstance(getConf, "Partitioning Job")
    val now = new Date

    FileInputFormat.addInputPath(job, new Path(args(0)))
    FileOutputFormat.setOutputPath(job, new Path(args(1), s"out_num_${now.getTime}"))

    job.setJarByClass(classOf[P4AppDriver])
    job.setMapperClass(classOf[Mapper[Any, Any, Any, Any]])
    job.setCombinerClass(classOf[NumberingReducer])
    job.setReducerClass(classOf[NumberingReducer])

    // Adding the partition files into distributed cache
    val fs = FileSystem.get(getConf)
    val files: RemoteIterator[LocatedFileStatus] = fs.listFiles(partitionsDir, false)
    while(files.hasNext) {
      job.addCacheFile(files.next().getPath.toUri)
    }

    job.setNumReduceTasks(args(2).toInt)
    job.setPartitionerClass(classOf[HashPartitioner[Any, Any]])

    job.setOutputKeyClass(classOf[LongWritable])
    job.setOutputValueClass(classOf[Text])
    job
  }
}


