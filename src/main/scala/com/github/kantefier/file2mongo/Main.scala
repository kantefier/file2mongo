package com.github.kantefier.file2mongo

//import scalaz._
//import Scalaz._
//import scalaz.stream._
//import scalaz.concurrent.Task

import java.nio.file.Paths

import akka._

import scala.io.Codec
import scala.util.{Failure, Try}
import org.mongodb.scala._
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler}
import akka.util.ByteString

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future, Promise}

object Main {
    def main(args: Array[String]): Unit = {
        println("Hello, world!")
//        val client = MongoClient()
//        val db = client.getDatabase("relevance")
//        val userColl = db.getCollection("users")

//        val fileSource = FileIO.fromPath(Paths.get("""/home/kinebogin/dev/dataset/seventy.data"""))
//        val fileSource = FileIO.fromPath(Paths.get("""D:\Dev\dataset\hundred""")).map(_.utf8String)
//        fileSource.via(collectToDocument).to(mongoSink(userColl))
//        client.close()
    }

    def finalPipeline(filePath: String) = FileIO.
      fromPath(Paths.get(filePath), chunkSize = 1000).
      via(Framing.delimiter(ByteString("\n"), 2000, allowTruncation = true)).
      map(_.utf8String).
      via(collectToDocument).
      to(Sink.fromGraph(MongoCommitSink("relevance", "users")))


    /**
     * Flow from lines to a Mongo Document, containing parsed user info
     */
    def collectToDocument: Flow[String, Document, NotUsed] = Flow[String]. //TODO: alas, it has to be overwritten
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

        val artistAndPlays: List[(String, Int)] = userLines.flatMap(line => parseSingleArtist(line).fold(_ => List.empty, List.apply(_)))

        if(artistAndPlays.length.toDouble / linesCount >= 0.8)
            Some(Document("userId" -> userIdent, "library" -> artistAndPlays.toList.map {
                case (artName, plays) => Document("artistName" -> artName, "plays" -> plays)
            }))
        else
            None
    }

    case class MongoCommitSink(database: String, collection: String) extends GraphStage[SinkShape[Document]] {
        val in: Inlet[Document] = Inlet("docInput")
        override val shape = SinkShape(in)

        override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
            private val clientPromise = Promise[MongoClient]()
            private lazy val client = Await.result(clientPromise.future, Duration("1 second"))
            private lazy val coll: MongoCollection[Document] = client.
              getDatabase(database).
              getCollection(collection)


            //open client session
            override def preStart(): Unit = {
                clientPromise.success(MongoClient())
                pull(in)
            }

            //close client session
            override def postStop(): Unit = {
                client.close()
                println("Closed MongoClient")
            }

            setHandler(in, new InHandler {
                override def onPush() = {
                    //TODO: subscribe to handle error events
                    coll.insertOne(grab(in)).subscribe((x: Completed) => println(x))
                    pull(in)
                }
            })
        }
    }

}