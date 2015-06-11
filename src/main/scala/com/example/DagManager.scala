package com.example

import akka.actor._
import akka.actor.ActorSystem
import akka.actor.Actor.Receive
import akka.util.Timeout
import scala.concurrent.Future
import scala.concurrent.Await
import akka.pattern.ask
import scala.concurrent.duration._
import akka.event.LoggingReceive

case class DagSpec(id : String, precursors : List[String], payload : String)

case class DagManager(input : List[DagSpec], sys : Option[ActorSystem] = None) {

  println("start..")
  assert(input.map(_.id).distinct.size == input.size, "Duplicate entries in the task list")
  val system = sys.getOrElse( ActorSystem("dag") )
  implicit val timeout = Timeout(5 seconds)
  val monitor = system.actorOf(Props[DagStatus],"monitor")
  println("monitor..")


  val dagDeps = input.map( node => node.precursors -> node.id )

  val dagRealized = input.map( node => {
    node.id -> system.actorOf( Props(classOf[DagNode],node.id,node.precursors,node.payload, monitor), node.id + "_executor")
  }).toMap

  val terminal_nodes = dagRealized.flatMap {
    case (id, node) => {
      dagDeps.filter(_._1.contains(id)).map(_._2) match {
        case depsIds: List[String] if depsIds.nonEmpty => {
          // this is where the node has dependencies
          node ! ConfigDeps(dagRealized.filter(n => depsIds.contains(n._1)).values.toList) // find the dependencies of each node
          None
        }
        case _ => {
          // these are the DAG end nodes
          node ! ConfigDeps(List[ActorRef]())
          Some(node)
        }
      }
    }
  }

  // find the start nodes and kick them
  val start_nodes = input.filter(_.precursors.isEmpty).map(_.id)
  dagRealized.foreach{ case (id,node) => {
    if(start_nodes.contains(id)){
      node ! Kick("$starter")
    }
  }}

  var completes = List[String]()
  do {
    val new_completes = Await.result( ask(monitor, DagCompletesQuery).mapTo[List[String]], 5 seconds ) //.mapTo[List[String]]
    if(new_completes.size > completes.size){
      println(s"${new_completes.diff(completes).head} reported complete - now total complete $new_completes")
      completes = new_completes
    }
  } while (completes.size != input.size)

  println("run completed")
  monitor ! PoisonPill

  system.shutdown()

  sys.getOrElse({
    system.awaitTermination()
  })

}

//messages
case class ConfigDeps( deps : List[ActorRef] )
case class Kick(from: String)
case class Cancel()
case class ReportStart(id : String, dag : ActorRef)
case class ReportDone(id : String, dag : ActorRef)
case class ReportError(id : String, dag : ActorRef)
case class DagCompletesQuery()

//actors
class DagNode(id:String, pre : List[String] ,payload : String, monitor : ActorRef ) extends Actor with akka.actor.ActorLogging  {
  import context._
  var post = List[ActorRef]()
  var kicks = List[String]()

  override def receive : Receive = LoggingReceive {
    case ConfigDeps(deps) => {
      post = deps
      println(s"${self.path.name} configured - becoming active")
      become(active)
    }
  }

  def active : Receive = LoggingReceive {
    case Kick(from) => {
      kicks = from :: kicks
      println(s"${self.path.name} kicked from $from - total kicks: \n\t${kicks} out of $pre " +
        s"so waiting for ${pre.diff(kicks)} kicks")
      if(pre.isEmpty) {  // this is a starter node
        runTask
      } else {
        pre.diff(kicks).size match {
          case 0 => {
            runTask
          }
          case 1 => monitor ! ReportStart(id, self)
          case _ => //between kick 1 and kicks complete
        }
      }
    }
  }

  def runTask = {
    println(payload)
    monitor ! ReportDone(id, self)
    post.foreach(_ ! Kick(id))
    self ! PoisonPill
  }
}

class DagStatus extends Actor with akka.actor.ActorLogging {

  var completes = Vector[String]()
  var working = Vector[ActorRef]()

  override def receive : Receive = LoggingReceive {
    case ReportDone(id, ref) => {
      completes = completes :+ id
      working = working.filterNot(_ == ref)
    }
    case ReportStart(id, ref) => {
      working = working :+ ref
    }
    case DagCompletesQuery => {
      sender ! completes.toList
    }
  }
}



