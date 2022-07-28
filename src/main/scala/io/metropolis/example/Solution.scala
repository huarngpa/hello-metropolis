package io.metropolis.example

import java.io.{BufferedWriter, File, FileWriter}

import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.stream.alpakka.csv.scaladsl.CsvParsing
import akka.stream.scaladsl.{FileIO, Flow, Sink, Source}
import akka.stream.{ActorAttributes, IOResult, Materializer, Supervision}
import akka.util.ByteString
import SolutionLoader.HashTag.HashTag
import SolutionLoader.MeasurementRecord
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, ExecutionContextExecutor, Future}
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

object Solution extends App {

  private val log = LoggerFactory.getLogger(this.getClass.getSimpleName)

  log.info("Script initialized...")

  implicit val system: ActorSystem = ActorSystem("Solution")
  implicit val ec: ExecutionContextExecutor = system.dispatcher

  val script = new SolutionLoader()
  Await.result(script.processMeasurements("/input.csv"), 10.minutes)
  Await.result(system.terminate(), 10.minutes)
}

class SolutionLoader(implicit ec: ExecutionContext, mat: Materializer) {

  private val log = LoggerFactory.getLogger(this.getClass.getSimpleName)

  def processMeasurements(fileName: String): Future[Done.type] = {

    var outputFiles: scala.collection.mutable.Map[String, BufferedWriter] = scala.collection.mutable.Map.empty

    val measurementSource: Source[ByteString, Future[IOResult]] =
      FileIO.fromPath(new File(getClass.getResource(fileName).getPath).toPath)

    measurementSource
      .via(CsvParsing.lineScanner())
      .map(_.map(_.utf8String.trim))
      .map(line => (line, Try(MeasurementRecord(line))))
      .via(SolutionLoader.logProgressFlow(count => log.info(s"Processed $count records")))
      .divertTo(Sink.foreach { case (line, tried) => log.warn(s"Could not parse line=$line - ${tried.failed.get.getMessage}") }, {
        case (_, triedRecord) => triedRecord.isFailure
      })
      .collect {
        case (_, Success(record)) => record
      }
      .groupBy(4, _.partition % 4)
      .map { record =>
        val recordedSum: Int = record.hashtags.map(SolutionLoader.hashtagConversion).sum
        val write = s"${record.sampleInstant},${record.uuid},$recordedSum\n"
        val outputFilename = s"output-file-${record.partition}.csv"
        val bw: BufferedWriter = outputFiles.get(outputFilename) match {
          case Some(writer) =>
            writer
          case None =>
            log.info(s"CREATED writer for $outputFilename")
            val writer = new BufferedWriter(new FileWriter(new File(outputFilename), false))
            outputFiles += (outputFilename -> writer)
            writer
        }
        bw.write(write)
        record
      }
      .mergeSubstreams
      .withAttributes(ActorAttributes.supervisionStrategy({
        case NonFatal(throwable) =>
          log.error(s"Encountered an error during processing - ${throwable.getMessage}", throwable)
          Supervision.Resume
      }))
      .runWith(Sink.ignore)
      .andThen {
        case Success(_)         => log.info(s"Finished processing measurements")
        case Failure(exception) => log.error(s"Could not finish processing, got ${exception.getMessage}", exception)

      }
      .map { _ =>
        outputFiles.values.foreach(_.close())
        Done
      }
  }
}

object SolutionLoader {
  object HashTag extends Enumeration {
    type HashTag = Value

    val `#one` = Value("#one")
    val `#two` = Value("#two")
    val `#three` = Value("#three")
    val `#four` = Value("#four")
    val `#five` = Value("#five")
    val `#six` = Value("#six")
    val `#seven` = Value("#seven")
    val `#eight` = Value("#eight")
    val `#nine` = Value("#nine")
    val `#ten` = Value("#ten")
  }

  val hashtagConversion = Map(
    HashTag.`#one` -> 1,
    HashTag.`#two` -> 2,
    HashTag.`#three` -> 3,
    HashTag.`#four` -> 4,
    HashTag.`#five` -> 5,
    HashTag.`#six` -> 6,
    HashTag.`#seven` -> 7,
    HashTag.`#eight` -> 8,
    HashTag.`#nine` -> 9,
    HashTag.`#ten` -> 10,
  )

  case class MeasurementRecord(sampleInstant: Long, partition: Int, uuid: String, hashtags: Seq[HashTag])

  object MeasurementRecord {
    def apply(line: Seq[String]): MeasurementRecord = {
      val hashtags = line.drop(3).map(HashTag.withName)
      MeasurementRecord(sampleInstant = line.head.toLong, partition = line(1).toInt, uuid = line(2), hashtags = hashtags)
    }
  }

  def logProgressFlow[T](logFn: BigInt => Unit, interval: Long = 1000): Flow[T, T, NotUsed] = {
    Flow[T].statefulMapConcat(() => {
      var processed: BigInt = 0
      element =>
        {
          processed += 1
          if (processed % interval == 0) {
            logFn(processed)
          }
          List(element)
        }
    })
  }
}
