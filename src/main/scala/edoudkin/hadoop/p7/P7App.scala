package edoudkin.hadoop.p7

import java.io.{FilenameFilter, File}
import java.nio.charset.StandardCharsets._

import org.apache.hive.hcatalog.streaming.{TransactionBatch, DelimitedInputWriter, HiveEndPoint}
import collection.JavaConverters._
import scala.io.Source

/**
 * Created by Egor Doudkin on 11/4/15.
 */
object P7App extends App {

  val columnNames = Array("bidid", "timestamp1", "ipinyouid", null, "useragent", "ip", "region", "city", "adexchange", "domain",
    "url", "anonymousurl", "adslotid", "adslotwidth", "adslotheight", "adslotvisibility", "adslotformat", "adslotfloorprice",
    "creativeid", "biddingprice", "advertiserid", "usertags")

  val INPUT_DIR_PATH = "/Users/egor/Documents/Hadoop_Training/Hadoop_Basics_p4/ipinyou.contest.dataset"
  val OUTPUT_ENDPOINT = ""

  def getDataStream(inputDirPath: String): Iterator[String] = {
    val inputDir = new File(inputDirPath)
    val inputFiles = inputDir.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String) = name.startsWith("imp.")
    })
    inputFiles.map(Source.fromFile).map(_.getLines()).reduce(_ ++ _)
  }

  def streamDataIntoHive(inputStream: Iterator[String], endpointUrl: String, schemaName: String, tableName: String): Unit = {

    val endpoint = new HiveEndPoint(endpointUrl, schemaName, tableName, null)
    val conn = endpoint.newConnection(false)
    val writer = new DelimitedInputWriter(columnNames, "\t", endpoint)

    // Storing 8 records per transaction and 64 transactions per batch

    val firstTxBatch = conn.fetchTransactionBatch(64, writer)

    val lastTxBatch = inputStream.grouped(8).foldLeft(firstTxBatch) { (txBatch, txRows) =>
      txBatch.beginNextTransaction()
      txBatch.write(txRows.map(_.getBytes(UTF_8)).asJavaCollection)
      txBatch.commit()

      if (txBatch.remainingTransactions() > 0) {
        // reuse the current tx batch for following rows of the input stream
        txBatch
      } else {
        // the tx batch is completely drained - close the current and start a new one
        txBatch.close()
        conn.fetchTransactionBatch(64, writer)
      }
    }

    lastTxBatch.close()
    conn.close()

  }

  println("Starting an input stream... ")
  val stream = getDataStream(INPUT_DIR_PATH)
  println("Streaming into Hive...")
  streamDataIntoHive(stream, schemaName = "flights", tableName = "ipinyou_streamable",
    endpointUrl = "thrift://sandbox.hortonworks.com:9083")
  println("Done!")

}