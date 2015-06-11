package com.example

import akka.actor.ActorSystem

object ApplicationMain extends App {
  val system = ActorSystem("DagActorSystem")

  val dag = List(DagSpec("a",List(),"payload a"),
    DagSpec("b",List("a"),"payload b"),
    DagSpec("c",List("a"),"payload c"),
    DagSpec("d",List("b","c"),"payload d"),
    DagSpec("e",List("d"),"payload e"))

  DagManager(dag,Some(system))
  system.awaitTermination()
}