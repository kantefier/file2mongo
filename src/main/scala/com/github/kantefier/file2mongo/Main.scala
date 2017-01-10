package com.github.kantefier.file2mongo

import scalaz._
import Scalaz._
import scalaz.stream._
import scalaz.concurrent.Task

object Main {
	def main(args: Array[String]): Unit = {
		println("Hello, world!")
	}

	def fileToChunks(filePath: String): Process[Task, Vector[String]] =
		io.linesR(filePath) |> process1.chunkBy2((x, y) => x.takeWhile(_ != '\t') === y.takeWhile(_ != '\t'))

}