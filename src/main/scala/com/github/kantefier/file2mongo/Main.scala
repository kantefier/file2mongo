package com.github.kantefier.file2mongo

import scalaz._
import Scalaz._
import scalaz.stream._
import scalaz.concurrent._

object Main {
	def main(args: Array[String]): Unit = {
		println("Hello, world!")
	}

	def TODORename(filePath: String) = {
		val fileStream: Process[Task, String] = io.linesR(filePath)
//		fileStream.map()
	}

//	def readUserLines()
}