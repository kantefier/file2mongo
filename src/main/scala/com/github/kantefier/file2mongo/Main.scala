package com.github.kantefier.file2mongo

import scalaz._
import Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task
import scala.io.Codec

object Main {
    def main(args: Array[String]): Unit = {
        println("Hello, world!")
    }

    def fileToChunks(filePath: String): Process[Task, Vector[String]] =
        io.linesR(filePath)(Codec.UTF8) |> process1.chunkBy2((x, y) => x.takeWhile(_ != '\t') === y.takeWhile(_ != '\t'))

    def commitUser(userLines: Vector[String]) = {
        //helpers
        val userId = "[0-9a-z]{40}"
        val singleUserLine = raw"""($userId)\s+(.*)""".r
        val artistMbId = "[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}"
        val artistWithMbid = raw"""($artistMbId)\s+(.*)""".r

        val userIdent = userLines.head.split("\\s").head

        //TODO: exception handling???
        def parseArtistInfo(artistInfo: String): (String, Int) = {
            val tokenized = artistInfo.split("\\s")
            tokenized.init.mkString(" ").trim -> tokenized.last.toInt
        }

        //TODO: exception handling???
        def parseSingleArtist(userLine: String): (String, Int) = userLine match {
            case singleUserLine(_, artistInfo) =>
                artistInfo match {
                    case artistWithMbid(_, artistInfo) => parseArtistInfo(artistInfo)
                    case artistWithoutMbid => parseArtistInfo(artistInfo)
                }
        }

        val artistAndPlays: Vector[(String, Int)] = userLines.map(parseSingleArtist)
        Process.emit(
            s"""{
               |userId: '$userIdent',
               |library:
               |${artistAndPlays.map { case (artName, plays) => s"{ artistName: '$artName', plays: $plays }" }.mkString("[", ",\n", "]") }
               |}""".stripMargin
        ).to(io.stdOutLines)
    }

}