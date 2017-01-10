lazy val root = (project in file(".")).
	settings(
		name := "File2Mongo",
		organization := "com.github.kantefier",
		version := "0.1",
		scalaVersion := "2.12.1",
		libraryDependencies ++=
			"org.mongodb.scala" %% "mongo-scala-driver" % "1.2.1" ::
			"org.scalaz" %% "scalaz-core" % "7.2.8" ::
			"org.scalaz.stream" %% "scalaz-stream" % "0.8.6" :: Nil
	)