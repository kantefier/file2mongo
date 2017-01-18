package com.github.kantefier.file2mongo

//import scalaz._
//import Scalaz._
//import scalaz.stream._
//import scalaz.concurrent.Task

import java.nio.file.Paths

import akka._
import scala.io.Codec
import scala.util.{Try, Failure}
import org.mongodb.scala._
import akka.stream._
import akka.stream.scaladsl._

object Main {
    def main(args: Array[String]): Unit = {
        println("Hello, world!")
//        FileIO.fromPath(Paths.get("""/home/kinebogin/dev/dataset/seventy.data"""))
    }

    /**
     * Flow from lines to a Mongo Document, containing parsed user info
     */
    def maflaw: Flow[String, Document, NotUsed] = Flow[String].
      groupBy(maxSubstreams = 100, _.takeWhile(_ != '\t')).
      fold(List.empty[String])((lines, currentLine) => currentLine :: lines).
      mapConcat(userLines => userDocFromLines(userLines).toList).
      mergeSubstreams


    /**
     *  Parse a Mongo Document from user lines
     */
    def userDocFromLines(userLines: List[String]): Option[Document] = {
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


        val artistAndPlays: List[(String, Int)] = userLines.flatMap(line => parseSingleArtist(line).fold(_ => List.empty, List.apply(_)))

        if(artistAndPlays.length.toDouble / linesCount >= 0.8)
            Some(Document("userId" -> userIdent, "library" -> artistAndPlays.toList.map {
                case (artName, plays) => Document("artistName" -> artName, "plays" -> plays)
            }))
        else
            None
    }

}