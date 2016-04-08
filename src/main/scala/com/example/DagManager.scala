package com.example

import akka.actor.Status.Success
import akka.actor._
import akka.actor.ActorSystem
import akka.actor.Actor.Receive
import akka.util.Timeout
import scala.annotation.tailrec
import scala.concurrent.Future
import scala.concurrent.Await
import akka.pattern.ask
import akka.pattern.pipe
import scala.concurrent.duration._
import akka.event.LoggingReceive

import scala.util.{Failure, Try}

case class DagSpec(id : String, precursors : List[String], payload : String, delay : Long = 0L, fail : Boolean = false)

case class DagManager(input : List[DagSpec], sys : Option[ActorSystem] = None) {

  println("start..")
  assert(input.map(_.id).distinct.size == input.size, "Duplicate entries in the task list")
  val system = sys.getOrElse( ActorSystem("dag") )
  implicit val timeout = Timeout(5 seconds)

  system.actorOf(Props(classOf[DagRunner],input),"runner")

//  sys.getOrElse({
//    system.awaitTermination()
//  })

}


class DagRunner( input : List[DagSpec] ) extends Actor with akka.actor.ActorLogging {

  val monitor = context.system.actorOf(Props(classOf[DagStatus],Vector(self)),"monitor")
  val dagDeps = input.map( node => node.precursors -> node.id )

  val dagRealized = input.map( node => {
    node.id -> context.system.actorOf(
      Props(classOf[DagNode],node.id,node.precursors,node.payload, monitor, node.delay, node.fail),
      node.id + "_executor")
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

  override def receive : Receive = LoggingReceive {
    case StartedNodes(n) => {
      println(s"started nodes : ${n}")
    }
    case CompleteNodes(n) => {
      completes = n
      println(s"reported complete : ${n}")
      if (completes.size == input.size) {
        println(s"Graph completed")
        monitor ! PoisonPill
        self ! PoisonPill
        context.system.terminate()
      }
    }
    case FailedNodes(n) => {
      if (n.nonEmpty) {
        println(s"Fail at $n \n\t aborting the graph!")
        dagRealized.foreach{ case (id,node) => node ! PoisonPill }
        monitor ! PoisonPill
        self ! PoisonPill
        context.system.terminate()
      }
    }
  }
}

//messages
case class Subscribe( ref : ActorRef )
case class CompleteNodes( nodes : List[String] )
case class StartedNodes( nodes : List[String] )
case class FailedNodes( nodes : List[String] )
case class ConfigDeps( deps : List[ActorRef] )
case class Kick(from: String)
case class Cancel()
case class ReportStart(id : String, dag : ActorRef)
case class ReportDone(id : String, dag : ActorRef)
case class ReportError(id : String, dag : ActorRef, cause : Throwable)
case class DagCompletesQuery()
case class DagFailuresQuery()

//actors
class DagNode(id:String, pre : List[String] ,payload : String, monitor : ActorRef, delay : Long, fail : Boolean)
  extends Actor with akka.actor.ActorLogging  {
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
        scheduleTask()
      } else {
        pre.diff(kicks).size match {
          case 0 => {
            scheduleTask()
          }
          case 1 => monitor ! ReportStart(id, self)
          case _ => //between kick 1 and kicks complete - keep waiting
        }
      }
    }
    case Success(res) => {
      monitor ! ReportDone(id, self)
      post.foreach(_ ! Kick(id))
      self ! PoisonPill
    }
    case Failure(ex) => {
      monitor ! ReportError(id,self,ex)
      self ! PoisonPill
    }
  }

  /* We only need to schedule this to simulate a delay - normally we would just do the work
   */
  def scheduleTask() = system.scheduler.scheduleOnce(delay milliseconds) {
    if(fail) {
      println(s"Simulated failure at ${self.path.name}")
      self ! Failure(new Exception("Born to fail"))
    } else {
      println(payload)
      self ! Success(payload)
    }
  }

}

class DagStatus( subscribers : Vector[ActorRef] ) extends Actor with akka.actor.ActorLogging {

  var completes = Vector[String]()
  var failures = Vector[String]()
  var working = Vector[ActorRef]()
  var clients = subscribers

  override def receive : Receive = LoggingReceive {
    case Subscribe( ref : ActorRef ) => {
      clients = clients :+ ref
    }
    case ReportDone(id, ref) => {
      completes = completes :+ id
      working = working.filterNot(_ == ref)
      clients.foreach( c => c ! CompleteNodes( completes.toList ) )
    }
    case ReportError(id, ref, cause) => {
      failures = failures :+ id
      working = working.filterNot(_ == ref)
      clients.foreach( c => c ! FailedNodes( failures.toList ) )
    }
    case ReportStart(id, ref) => {
      working = working :+ ref
      clients.foreach( c => c ! StartedNodes( working.map(_.path.name).toList ) )
    }
    case DagCompletesQuery => {
      sender ! completes.toList
    }
    case DagFailuresQuery => {
      sender ! failures.toList
    }
  }
}



