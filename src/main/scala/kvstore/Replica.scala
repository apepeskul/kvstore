package kvstore

import akka.actor._
import kvstore.Arbiter._
import kvstore.Replicator.SnapshotAck
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.Restart
import scala.annotation.tailrec
import akka.pattern.{ ask, pipe }
import scala.concurrent.duration._
import akka.util.Timeout

object Replica {
  sealed trait Operation {
    def key: String
    def id: Long
  }
  case class Insert(key: String, value: String, id: Long) extends Operation
  case class Remove(key: String, id: Long) extends Operation
  case class Get(key: String, id: Long) extends Operation

  sealed trait OperationReply
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}

class Replica(val arbiter: ActorRef, persistenceProps: Props) extends Actor {
  import Replica._
  import Replicator._
  import Persistence._
  import context.dispatcher

  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */
  
  var kv = Map.empty[String, String]
  // a map from secondary replicas to replicators
  var secondaries = Map.empty[ActorRef, ActorRef]
  // the current set of replicators
  var replicators = Set.empty[ActorRef]
  var seq: Long= 0L

  val persistor = context.actorOf(persistenceProps)
  var persistencePrimaryMap: Map[Long, (Cancellable, ActorRef)] = Map.empty[Long, (Cancellable, ActorRef)]
  var persistenceSecondaryMap: Map[Long, (Cancellable, ActorRef)] = Map.empty[Long, (Cancellable, ActorRef)]
  var primaryFailedMap = Map.empty[Long, Cancellable]
  var secondaryFailedMap = Map.empty[Long, Cancellable]
  var replicatorPendingOperation = Map.empty[Long, scala.collection.mutable.Seq[ActorRef]].withDefaultValue(Nil)

  arbiter ! Join


  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica)
  }

  /* TODO Behavior for  the leader role. */
  val leader: Receive = {
    case Get(k, i) => sender ! GetResult(k, kv.lift(k),i)
    case Insert(key, value, id) => {
      kv+= key-> value
      replicators.map(replicator => {
        replicator ! Replicate(key, Some(value), id)
        val oldReplicators = replicatorPendingOperation(id)
        replicatorPendingOperation = replicatorPendingOperation.updated(id, oldReplicators :+ replicator)
      })

      primaryFailedMap += id -> context.system.scheduler.scheduleOnce(1 seconds, sender, OperationFailed(id))
      persistencePrimaryMap += id -> (context.system.scheduler.schedule(Duration.Zero, 200 microseconds, persistor, Persist(key, Some(value), id)), sender)
      }
    case Remove(key, id) => {
      kv -= key
      replicators.map(_ ! Replicate(key, None, id))
      primaryFailedMap += id -> context.system.scheduler.scheduleOnce(1 seconds, sender, OperationFailed(id))
      persistencePrimaryMap += id ->(context.system.scheduler.schedule(Duration.Zero, 200 microseconds, persistor, Persist(key, None, id)), sender)
    }
    case SnapshotAck(k, seq) => {
      val oldReplicators = replicatorPendingOperation(seq)
      replicatorPendingOperation = replicatorPendingOperation.updated(seq, oldReplicators.filter(_ == sender))
      if (replicatorPendingOperation(seq).isEmpty) {
        replicatorPendingOperation - seq
        persistencePrimaryMap(seq)._2 ! OperationAck(seq)
      }
    }
    case Persisted(key, id) if replicatorPendingOperation(id).isEmpty => {
      persistencePrimaryMap(id)._1.cancel()
      primaryFailedMap(id).cancel()
      persistencePrimaryMap(id)._2 ! OperationAck(id)
      persistencePrimaryMap - id

    }
    case Replicas(replicas) => {
      val replicasToProcess = replicas - self
      val (oldSet, newSet)= replicasToProcess partition (secondaries.values.toSet contains _)
      newSet.foreach {repl =>
        val replicator = context.actorOf(Replicator.props(repl))
        replicators += replicator
        kv.zipWithIndex.map{case ((k, v), idx) =>
          replicator ! Replicate(k, Some(v), idx)}
        secondaries += repl -> replicator
      }
    }
    case _ =>
  }

  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(k, i) => sender ! GetResult(k, kv.lift(k),i)
    case Snapshot(k, v, id) => {
      if(seq > id) sender ! SnapshotAck(k, id) else if (seq == id) {
        seq+=1
        v match {
          case Some(v) => {
            kv= kv.updated(k, v)
          }
          case _ => kv -= k
        }
        secondaryFailedMap += id -> context.system.scheduler.scheduleOnce(1 seconds, sender, OperationFailed(id))
        persistenceSecondaryMap += id -> (context.system.scheduler.schedule(Duration.Zero, 200 microseconds, persistor, Persist(k, v, id)), sender)
      }

      }
    case Persisted(key, id) => {
      persistenceSecondaryMap(id)._1.cancel()
      secondaryFailedMap(id).cancel()
      persistenceSecondaryMap(id)._2 ! SnapshotAck(key, id)
      persistenceSecondaryMap - id
    }
    case _ =>
  }

}

