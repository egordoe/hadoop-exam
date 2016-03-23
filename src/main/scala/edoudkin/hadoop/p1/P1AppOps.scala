package edoudkin.hadoop.p1
import java.io.PrintWriter

import com.jamonapi.MonitorFactory
import org.apache.hadoop.fs.{FileStatus, Path}

import scala.io.Source

/**
 * Created by Egor Doudkin on 9/29/15.
 */
trait P1AppOps {
  def getOccurrences(filePath: String, initOccurrences: Map[String, Int], idPredicate: (String) => Boolean): Map[String, Int]
  def sortOccurrences(occurences: Map[String, Int]): Seq[(String, Int)]
  def combinePartitionFiles(part1File: Path, part2File: Path, outputDir: Path): Path
  def writePartitionFiles(inputDirPath: String, outputDirPath: String): Seq[Path]
}

trait LoggedP1AppOps extends P1AppOps {

  abstract override def getOccurrences(filePath: String, initOccurrences: Map[String, Int], idPredicate: (String) => Boolean): Map[String, Int] = {
    val runtime = Runtime.getRuntime
    println(s"Analyzing '$filePath' file...")
    val mon = MonitorFactory.start("getOccurrences")
    try {
      super.getOccurrences(filePath, initOccurrences, idPredicate)
    } finally {
      mon.stop()
      println(s"timeTaken: ${mon.getLastValue/1000}s, memory: ${(runtime.totalMemory() - runtime.freeMemory())/1024} kB")
    }

  }

  abstract override def sortOccurrences(occurrences: Map[String, Int]): Seq[(String, Int)] = {
    val runtime = Runtime.getRuntime
    println(s"Sorting (size='${occurrences.size})...")
    val mon = MonitorFactory.start("sortOccurrences")
    try {
      super.sortOccurrences(occurrences)
    } finally {
      mon.stop()
      println(s"timeTaken: ${mon.getLastValue/1000}s, memory: ${(runtime.totalMemory() - runtime.freeMemory())/1024} kB")
    }
  }

  abstract override def combinePartitionFiles(part1File: Path, part2File: Path, outputDir: Path): Path = {
    val runtime = Runtime.getRuntime
    println(s"Combining '${part1File.getName}' and '${part2File.getName}' partitions")
    val mon = MonitorFactory.start("combinePartitionFiles")
    try {
      super.combinePartitionFiles(part1File, part2File, outputDir)
    } finally {
      mon.stop()
      println(s"timeTaken: ${mon.getLastValue/1000}s, memory: ${(runtime.totalMemory() - runtime.freeMemory())/1024} kB")
    }
  }

  abstract override def writePartitionFiles(inputDirPath: String, outputDirPath: String): Seq[Path] = {
    val runtime = Runtime.getRuntime
    println(s"Writing partitions input='$inputDirPath' output='$outputDirPath' partitions")
    val mon = MonitorFactory.start("writePartitionFiles")
    try {
      super.writePartitionFiles(inputDirPath, outputDirPath)
    } finally {
      mon.stop()
      println(s"timeTaken: ${mon.getLastValue/1000}s, memory: ${(runtime.totalMemory() - runtime.freeMemory())/1024} kB")
    }
  }
}
