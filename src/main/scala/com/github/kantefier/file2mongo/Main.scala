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
    def main(args: Array[String]): Unit = Try {
        val argMap = argsToMap(args)
        val datasetPath = argMap.get("-dataset")
        val db = argMap.get("-db")
        val coll = argMap.get("-coll")

        require(datasetPath.isDefined, """Argument -dataset has to be specified. Example: -dataset=D:\dataset\datasetFile""")
        require(db.isDefined, "Argument -db has to be specified. Example: -db=relevance")
        require(coll.isDefined, "Argument -coll has to be specified. Example: -coll=users")

        val system = ActorSystem()
        val mat = ActorMaterializer()(system)
        finalPipeline(datasetPath.get, db.get, coll.get).run()(mat).onComplete(_ => system.terminate())(mat.executionContext)
    } recover {
        case argEx: IllegalArgumentException => println(argEx.getMessage)
        case ex: Throwable =>
            println("Something went wrong, my friend...")
            ex.printStackTrace()
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
      * Returns a complete pipeline that reads, parses and commits and dataset to MongoDB
      */
    def finalPipeline(filePath: String, dbName: String, collName: String) = FileIO.
      fromPath(Paths.get(filePath), chunkSize = 1000).
      via(Framing.delimiter(ByteString("\n"), 2000, allowTruncation = true)).
      map(_.utf8String).
      via(Flow.fromGraph(new UserLinesCollector)).
      via(linesToDocument).
      to(Sink.fromGraph(MongoCommitSink(dbName, collName)))

    /**
     * Flow from lines to a Mongo Document, containing parsed user info
     */
    def linesToDocument: Flow[List[String], BSONDocument, NotUsed] = Flow[List[String]].
      mapConcat(userLines => userDocFromLines(userLines).toList)


    /**
      * Parses single user line into tuple (ArtistName, PlaysCount)
      */
    def parseSingleArtist(userLine: String): Try[(String, Int)] = {
        val userId = "[0-9a-z]{40}"
        val singleUserLine = raw"""($userId)\s+(.*)""".r
        val artistMbId = "[0-9a-z]{8}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{4}-[0-9a-z]{12}"
        val artistWithMbid = raw"""($artistMbId)\s+(.*)""".r

        def parseArtistInfo(artistInfo: String): Try[(String, Int)] = {
            val tokenized = artistInfo.split("\\s")
            Try(tokenized.init.mkString(" ").trim -> tokenized.last.toInt)
        }

        userLine match {
            case singleUserLine(_, artistInfo) =>
                artistInfo match {
                    case artistWithMbid(_, artistInfo) => parseArtistInfo(artistInfo)
                    case artistWithoutMbid => parseArtistInfo(artistInfo)
                }
            case failedLine => Failure(new Exception(s"Couldn't parse line: <<$failedLine>>"))
        }
    }

    /**
     *  Parse a Mongo Document from user lines
     */
    def userDocFromLines(userLines: List[String]): Option[BSONDocument] = {
        val linesCount = userLines.length
        val userIdent = userLines.head.split("\\s").head

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

                /**
                  * Inits Mongo driver and connection
                  */
                override def preStart(): Unit = MongoConnection.parseURI("mongodb://localhost") match {
                    case Success(parsedUri) =>
                        println("MongoDriver init")
                        driver = MongoDriver()
                        connection = driver.connection(parsedUri)
                        pull(in)
                    case Failure(ex) =>
                        failStage(ex)
                }

                /**
                  * Close driver and all connections
                  */
                override def postStop(): Unit = {
                    println("MongoSink: signalling about completetion")
                    finishPromise.success(Done)
                    println("Closing MongoDriver")
                    driver.close()
                    println("Closed MongoDriver")
                }

                setHandler(in, new InHandler {
                    /**
                      * Receives a document and commits it
                      * Awaits for the Future to complete, then pulls another doc
                      */
                    override def onPush() = {
                        val doc = grab(in)
                        implicit val ec = materializer.executionContext
                        val dbAction = for {
                            db <- connection.database(database)
                            insertRes <- db.collection[BSONCollection](collection).insert(doc)
                            _ <- Future(if (!insertRes.ok) {
                                println(insertRes.writeErrors.map(_.errmsg).mkString(";"))
                            })
                        } yield ()
                        Await.ready(dbAction, Duration("2 seconds"))
                        if(!hasBeenPulled(in)) pull(in)
                    }
                })
            }
            (logic, finishPromise.future)
        }
    }

    /**
      * Collects input Strings into a List[String] based on the built-in criteria
      */
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