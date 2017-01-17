package com.github.kantefier.file2mongo

import scalaz._
import Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import scala.io.Codec
import scala.util.{Try, Failure}
import org.mongodb.scala._

object Main {
    def main(args: Array[String]): Unit = {
        println("Hello, world!")
    }

    def process(fileName: String) = {
//        val db = MongoClient()
        (io.linesR(fileName)(Codec.UTF8) |> fileToChunks).flatMap(prepareUserDoc)
    }

    def fileToChunks =
        process1.chunkBy2[String]((x, y) => x.takeWhile(_ != '\t') === y.takeWhile(_ != '\t'))

    def prepareUserDoc(userLines: Vector[String]): Process[Task, Document] = {
        //helpers
        val userId = "[0-9a-z]{40}"
        val singleUserLine = raw"""($userId)\s+(.*)""".r
        val artistMbId = "[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}"
        val artistWithMbid = raw"""($artistMbId)\s+(.*)""".r

        val linesCount = userLines.length
        val userIdent = userLines.head.split("\\s").head

        def parseArtistInfo(artistInfo: String): Try[(String, Int)] = {
            val tokenized = artistInfo.split("\\s")
            Try(tokenized.init.mkString(" ").trim -> tokenized.last.toInt)
        }

        def parseSingleArtist(userLine: String): Try[(String, Int)] = userLine match {
            case singleUserLine(_, artistInfo) =>
                artistInfo match {
                    case artistWithMbid(_, artistInfo) => parseArtistInfo(artistInfo)
                    case artistWithoutMbid => parseArtistInfo(artistInfo)
                }
            case failedLine => Failure(new Exception(s"Couldn't parse line: <<$failedLine>>"))
        }

        /*val printObserver = new Observer[Completed] {
            override def onNext(result: Completed): Unit = println(s"onNext: $result")
            override def onError(e: Throwable): Unit = println(s"onError: $e")
            override def onComplete(): Unit = println("onComplete")
        }*/

        val artistAndPlays: Vector[(String, Int)] = userLines.flatMap(line => parseSingleArtist(line).fold(_ => Vector.empty, Vector.apply(_)))

        if(artistAndPlays.length.toDouble / linesCount >= 0.8)
            Process.emit {
                Document("userId" -> userIdent, "library" -> artistAndPlays.toList.map {
                    case (artName, plays) => Document("artistName" -> artName, "plays" -> plays)
                })
            }
        else
            Process.halt
    }

}