package com.example

import java.io.File
import scala.io.Source
import akka.actor.ActorSystem
import org.json4s._
import org.json4s.jackson.JsonMethods._

object ApplicationMain extends App {
  implicit val formats = DefaultFormats

  val dag = args.length match {
    case 1 if new File(args(0)).exists() => {
      parse( Source.fromFile(new File(args(0)),"utf-8").mkString ).extract[List[DagSpec]]
    }
    case _ => List(DagSpec("a",List(),"payload a"),
      DagSpec("b",List("a"),"payload b"),
      DagSpec("c",List("a"),"payload c"),
      DagSpec("d",List("b","c"),"payload d"),
      DagSpec("e",List("d"),"payload e"))
  }

  val system = ActorSystem("DagActorSystem")

  println(s"input ${dag.mkString("\n")}")

  DagManager(dag,Some(system))
  system.awaitTermination()
}