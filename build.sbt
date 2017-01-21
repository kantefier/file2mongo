lazy val root = (project in file(".")).
	settings(
		name := "File2Mongo",
		organization := "com.github.kantefier",
		version := "0.1",
		scalaVersion := "2.12.1",
		libraryDependencies ++=
			"org.reactivemongo" %% "reactivemongo" % "0.12.1" ::
//			"org.mongodb.scala" %% "mongo-scala-driver" % "1.2.1" ::
		  	"com.typesafe.akka" %% "akka-actor" % "2.4.16" ::
		  	"com.typesafe.akka" %% "akka-stream" % "2.4.16" ::
			"ch.qos.logback"    %   "logback-classic"   %   "1.1.7" :: Nil
	)
