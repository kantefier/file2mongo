package com.github.kantefier.file2mongo

import java.nio.file.Paths

import akka._
import akka.actor.ActorSystem
import reactivemongo.api.commands.WriteResult
import reactivemongo.bson.BSONDocument

import scala.concurrent.duration.FiniteDuration
import scala.io.Codec
import scala.util.{Failure, Success, Try}
import akka.stream._
import akka.stream.scaladsl._
import akka.stream.stage._
import akka.util.ByteString
import reactivemongo.bson._
import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object Main {
    def main(args: Array[String]): Unit = {
        val argMap = argsToMap(args)
        argMap.get("-dataset") match {
            case Some(path) =>
                val system = ActorSystem()
                val mat = ActorMaterializer()
                finalPipeline(path).run()(mat).onComplete(_ => system.terminate())(mat.executionContext)

            case _ => println("Not used correctly. Doing nothing.")
        }
    }

    /**
      * Parses command line arguments, separated by space.
      * Example of an argument: -argName=argValue
      */
    def argsToMap(args: Array[String]): Map[String, String] = args.toList.flatMap(_.split('=').toList match {
        case argName :: argValue => (argName -> argValue.mkString) :: Nil
        case _ => Nil
    }).toMap

    /**
      * Assembled streaming pipeline
      */
    def finalPipeline(filePath: String) = FileIO.
      fromPath(Paths.get(filePath), chunkSize = 1000).
      via(Framing.delimiter(ByteString("\n"), 2000, allowTruncation = true)).
      map(_.utf8String).
      via(Flow.fromGraph(new UserLinesCollector)).
      via(linesToDocument).
      to(Sink.fromGraph(MongoCommitSink("relevance", "test")))

    /**
     * Flow from lines to a Mongo Document, containing parsed user info
     */
    def linesToDocument: Flow[List[String], BSONDocument, NotUsed] = Flow[List[String]].
      mapConcat(userLines => userDocFromLines(userLines).toList)


    /**
     *  Parse a Mongo Document from user lines
     */
    def userDocFromLines(userLines: List[String]): Option[BSONDocument] = {
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
            Some(BSONDocument("userId" -> userIdent, "library" -> artistAndPlays.toList.map {
                case (artName, plays) => BSONDocument("artistName" -> artName, "plays" -> plays)
            }))
        else None
    }

    /**
      * Sink to commit documents to specified database and collection. Initiates Mongo Driver on start
      */
    case class MongoCommitSink(database: String, collection: String) extends GraphStageWithMaterializedValue[SinkShape[BSONDocument], Future[Done]] {
        val in: Inlet[BSONDocument] = Inlet("docInput")
        override val shape = SinkShape(in)

        override def createLogicAndMaterializedValue(inheritedAttributes: Attributes): (GraphStageLogic, Future[Done]) = {
            val finishPromise = Promise[Done]()
            val logic = new GraphStageLogic(shape) {

                var driver: MongoDriver = _
                var connection: MongoConnection = _

                //open client session
                override def preStart(): Unit = MongoConnection.parseURI("mongodb://localhost") match {
                    case Success(parsedUri) =>
                        println("MongoDriver init")
                        driver = MongoDriver()
                        connection = driver.connection(parsedUri)
                        pull(in)
                    case Failure(ex) =>
                        failStage(ex)
                }

                //close client session
                override def postStop(): Unit = {
                    println("MongoSink: signalling about completetion")
                    finishPromise.success(Done)
                    println("Closing MongoDriver")
                    driver.close()
                    println("Closed MongoDriver")
                }

                def errPrint(wr: WriteResult): Unit = if (!wr.ok) {
                    println(wr.writeErrors.map(_.errmsg).mkString(";"))
                }

                setHandler(in, new InHandler {
                    override def onPush() = {
                        val doc = grab(in)
                        implicit val ec = materializer.executionContext
                        val dbAction = for {
                            db <- connection.database(database)
                            insertRes <- db.collection[BSONCollection](collection).insert(doc)
                            _ <- Future(errPrint(insertRes))
                        } yield ()
                        Await.ready(dbAction, Duration("2 seconds"))
                        pull(in)
                    }
                })
            }
            (logic, finishPromise.future)
        }
    }

    class UserLinesCollector extends GraphStage[FlowShape[String, List[String]]] {
        val in: Inlet[String] = Inlet("inLine")
        val out: Outlet[List[String]] = Outlet("outLines")

        val shape = FlowShape(in, out)

        override def createLogic(inheritedAttributes: Attributes) = new GraphStageLogic(shape) {
            var isFirstIteration = true
            var currentUserIdent: String = ""
            var userLinesAccum: List[String] = List.empty

            setHandler(in, new InHandler {
                override def onPush() = {
                    val str = grab(in)
                    if(isFirstIteration) {
                        userLinesAccum = str :: Nil
                        currentUserIdent = str.takeWhile(_ != '\t')
                        isFirstIteration = false
                    } else if(str.startsWith(currentUserIdent)) {
                        userLinesAccum = str :: userLinesAccum
                    } else {
                        push(out, userLinesAccum)
                        userLinesAccum = str :: Nil
                        currentUserIdent = str.takeWhile(_ != '\t')
                    }
                    if(!hasBeenPulled(in)) pull(in)
                }
            })

            setHandler(out, new OutHandler {
                override def onPull() = {
                    if(!hasBeenPulled(in)) pull(in)
                }
            })
        }
    }

}