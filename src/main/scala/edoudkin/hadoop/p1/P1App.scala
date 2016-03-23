package edoudkin.hadoop.p1

import java.io.PrintWriter
import java.net.URI

import com.jamonapi.MonitorFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, FileSystem}
import scala.collection.immutable.{ListMap, HashMap}
import scala.io.Source

/**
 * Created by Egor Doudkin on 9/29/15.
 */
object P1App extends App {

  val app = new P1App("hdfs://sandbox.hortonworks.com:8020") with LoggedP1AppOps
  app.writeAggOccurrences("/user/training/1/", "/user/training/1/result/")

}

class P1App(val defaultFS: String) extends P1AppOps {

  val defaultFSdUri = URI.create(defaultFS)

  val config = new Configuration()
  val fs = FileSystem.get(defaultFSdUri, config)

  def list(path: String): Seq[Path] = {
    val listing = fs.listFiles(new Path(path), false)
    var files = Seq.empty[FileStatus]
    while (listing.hasNext) {
      val file = listing.next()
      if (!file.isDirectory) {
        files = files :+ file
      }
    }
    files.map(_.getPath)
  }

  def getOccurrences(filePath: String, initOccurrences: Map[String, Int], partitionPred: (String) => Boolean): Map[String, Int] = {
    val ids: Iterator[String] = for {
      line <- Source.fromInputStream(fs.open(new Path(filePath))).getLines()
    } yield line.split('\t')(2) // take the 3rd column where ids are

    ids.foldLeft(initOccurrences) { (occurrences, id) =>
      if (partitionPred(id)) {
        val occurrence = occurrences.getOrElse(id, 0) + 1
        occurrences.updated(id, occurrence)
      } else {
        occurrences
      }
    }
  }

  def getAggOccurrences(filePaths: Seq[Path], partitionPred: (String) => Boolean): Seq[(String, Int)] = {

    val unsorted = filePaths.foldLeft(Map.empty[String, Int]) { (occurrences, filePath) =>
      getOccurrences(filePath.toString, occurrences, partitionPred)
    }
    sortOccurrences(unsorted)

  }

  def sortOccurrences(occurrences: Map[String, Int]): Seq[(String, Int)] = occurrences.toSeq.sortBy(-_._2)

  def writePartitionFiles(inputDirPath: String, outputDirPath: String): Seq[Path] = {

    println(s"Input Directory: $inputDirPath")
    val inputFilePaths = list(inputDirPath)
    for (name <- inputFilePaths.map(_.getName)) {
      println("\t" + name)
    }

    val outputDir = new Path(outputDirPath)

    if (!fs.exists(outputDir)) {
      fs.mkdirs(outputDir)
    }

    println(s"Output Directory: $outputDirPath")
    val outputFilePaths = list(outputDirPath)
    for (name <- outputFilePaths.map(_.getName)) {
      println("\t" + name)
    }

    // this function classifies an id to belong to one of 8 partitions.
    // The partitioning happens based on the last character of an id.
    def partitionPredicate(partitionNum: Int)(id: String): Boolean = {
      val hash = id(id.length - 1) & 0x7
      hash == partitionNum
    }

    // go over every partition and store results into partition file.
    for (partitionNum <- 0 to 7) yield {
      val partition = getAggOccurrences(inputFilePaths, partitionPredicate(partitionNum))
      val partitionPath = new Path(outputDir, s"bid_part_$partitionNum.txt")
      val out = new PrintWriter(fs.create(partitionPath, true))

      try {
        for ((id, occurrence) <- partition) {
          out.println(s"$id\t$occurrence")
        }
      } finally {out.close()}

      partitionPath
    }

  }

  def combinePartitionFiles(part1File: Path, part2File: Path, outputDir: Path): Path = {
    val combinedPath = new Path(outputDir, part1File.getName.split("\\.")(0) + part2File.getName.split("\\.")(0) + ".txt")

    val occurrences1 = Source.fromInputStream(fs.open(part1File)).getLines().map { (line) =>
      val occurrence = line.split("\t")(1).toInt
      occurrence -> line
    }
    val occurrences2: Iterator[(Int, String)] = Source.fromInputStream(fs.open(part2File)).getLines().map { (line) =>
      val occurrence = line.split("\t")(1).toInt
      occurrence -> line
    }

    val out = new PrintWriter(fs.create(combinedPath, true))

    try {

      var occ1Val: Option[(Int, String)] = None
      var occ2Val: Option[(Int, String)] = None

      while(occurrences1.hasNext || occurrences2.hasNext || occ1Val.isDefined || occ2Val.isDefined) {
        occ1Val = if (occ1Val.isEmpty && occurrences1.hasNext) Some(occurrences1.next()) else occ1Val
        occ2Val = if (occ2Val.isEmpty && occurrences2.hasNext) Some(occurrences2.next()) else occ2Val

        (occ1Val, occ2Val) match {
          case (Some((_, line)), None) =>
            out.println(line)
            occ1Val = None
          case (None, Some((_, line))) =>
            out.println(line)
            occ2Val = None
          case (Some((occ1, line)), Some((occ2, _))) if occ1 >= occ2 =>
            out.println(line)
            occ1Val = None
          case (Some((occ1, _)), Some((occ2, line))) if occ1 < occ2 =>
            out.println(line)
            occ2Val = None
        }

      }

    } finally {out.close()}

    combinedPath
  }

  def writeAggOccurrences(inputDirPath: String, outputDirPath: String): Unit = {

    val outputDir = new Path(outputDirPath)
   val partitionFiles = writePartitionFiles(inputDirPath, outputDirPath)

    val aggFile = partitionFiles.reduce { (path1, path2) =>
      combinePartitionFiles(path1, path2, new Path(outputDirPath))
    }

    println(s"Done! $aggFile" )

  }

}
