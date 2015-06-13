package kvstore

import akka.actor._
import kvstore.Arbiter._
import kvstore.Persistence.{Persisted, Persist}
import kvstore.Replica._
import kvstore.Replicator.{Replicated, Replicate}
import scala.collection.immutable.Queue
import akka.actor.SupervisorStrategy.{Stop, Escalate, Restart}
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
  case class TryPersist2(id:Long)
  case class TryPersist(remaining:Int)

  object Success
  sealed trait OperationReply
  case class TimedOut(id:Long)
  case class RemoveReplicator(replicator:ActorRef)
  case class OperationAck(id: Long) extends OperationReply
  case class OperationFailed(id: Long) extends OperationReply
  case class GetResult(key: String, valueOption: Option[String], id: Long) extends OperationReply

  def props(arbiter: ActorRef, persistenceProps: Props): Props = Props(new Replica(arbiter, persistenceProps))
}


class AckActor(persistence:ActorRef, replicators:Set[ActorRef], persist: Persist, replyTo:ActorRef) extends Actor {
  import scala.concurrent.ExecutionContext.Implicits.global

  replicators.foreach( r => r ! Replicate(persist.key, persist.valueOption, persist.id))
  setupWaitAcks()

  var remainingAcks = replicators
  var selfReplicated = false
  var inactive :Receive = {
    case _ =>
  }

  def checkForAllAcked(id:Long) = {
    if (remainingAcks.isEmpty && selfReplicated ) {
      replyTo.tell(OperationAck(id), context.parent)
      context.become(inactive)
      self ! PoisonPill
    }
  }

  def setupWaitAcks() = {
    context.system.scheduler.scheduleOnce(1500.millis, self, Timeout)
  }


  def receive: Actor.Receive = {
    case RemoveReplicator(replicator) =>
      remainingAcks = remainingAcks - replicator
      checkForAllAcked(persist.id)

    case TryPersist(remaining) =>

      implicit val timeout = Timeout(100.millis)
      (persistence ? persist).mapTo[Persisted]
        .map { p =>
        println("Received Persisted")
        self ! Success
      }.recover { case ex => self ! TryPersist(remaining - 1)}

    case Replicated(key, id) =>
      println("Received Replicated")
      remainingAcks = remainingAcks - sender()
      checkForAllAcked(id)

    case Success =>
      println("Received Success")
      selfReplicated = true
      checkForAllAcked(persist.id)

    case Timeout =>
      println("Received Timeout")
      context.become(inactive)
      self ! PoisonPill
      replyTo.tell(OperationFailed(persist.id), context.parent)
  }
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

  arbiter ! Join

  def receive = {
    case JoinedPrimary   => context.become(leader)
    case JoinedSecondary => context.become(replica )
  }

  override val supervisorStrategy =
    OneForOneStrategy() {
      case _: PersistenceException      => Stop
      case _: Exception                => Escalate
    }

  var persistence: ActorRef = _

  def createPersistence() = {
    persistence = context.actorOf(persistenceProps)
    context.watch(persistence)
  }
  createPersistence()
  /* TODO Behavior for  the leader role. */

  var ackActors = Map[Long, ActorRef]()

  def spawn(id:Long, key:String, value:Option[String]) = {
    ackActors = ackActors + (id -> context.actorOf(Props(classOf[AckActor],
      persistence, replicators, Persist(key, value, id), sender())))
    ackActors(id) ! TryPersist(10)
    context.watch(ackActors(id))
  }

  val leader: Receive = {


    case Insert(key, value, id) =>
      println("Received Insert")
      kv = kv + (key -> value)
      spawn(id, key, Some(value))

    case Remove(key, id) =>
      println("Received Remove")
      kv = kv - key
      spawn(id, key, None)

    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case Replicas(replicaSet) =>
      println(s"Received a new replica set with: ${replicaSet.size} total.")
      for (replica <- replicaSet) {

        if (!secondaries.keys.toList.contains(replica) && replica != self){
          println("Replica Joined")
          val replicator = context.actorOf(Replicator.props(replica))
          replicators = replicators + replicator
          secondaries = secondaries + (replica -> replicator)
          kv.foreach { case (key, value) => replicator ! Replicate(key, Some(value), 0)}
        }
      }
      for (oldReplica <- secondaries.keys.toList) {
        if(!replicaSet.contains(oldReplica)) {
          println("Replica Left")
          val replicator = secondaries(oldReplica)
          replicator ! PoisonPill
          ackActors.values.toList.foreach(ack => ack ! RemoveReplicator(replicator))
          replicators = replicators - replicator
          secondaries = secondaries - oldReplica
        }
      }
    case Terminated(actor) if actor == persistence =>
      println("Persistence had exception.")
      createPersistence()
    case Terminated(actor) if ackActors.values.toList.contains(actor) =>
      println("Ack actor was terminated: " + actor.path.name)
      val Some((id, _)) = ackActors.find(_._2 == actor)
      ackActors = ackActors - id
  }
  var toPersist = Map[Long, Persist]()
  var waiting = Map[Long, ActorRef]()
  var cancellables = Map[Long, Cancellable]()

  var expected = 0L

  def cleanup(id:Long) = {
    toPersist = toPersist - id
    cancellables = cancellables - id
    waiting = waiting - id
  }
  /* TODO Behavior for the replica role. */
  val replica: Receive = {
    case Get(key, id) =>
      sender() ! GetResult(key, kv.get(key), id)

    case s@Snapshot(key, valOption, seq) =>
      println(s"Replica ${self.path.name} received " + s)
      if(seq > expected) {
        // do nothing
      } else if(seq < expected) {
        sender() ! SnapshotAck(key, seq)
      } else {
        valOption match {
          case Some(value) => kv = kv + (key -> value)
          case None => kv = kv - key
        }
        expected = math.max(expected, seq + 1)
        val canc = context.system.scheduler.scheduleOnce(1000.millis, self, TimedOut(seq))
        toPersist = toPersist + (seq -> Persist(key, valOption, seq))
        waiting = waiting + (seq -> sender())
        cancellables = cancellables + (seq -> canc)
        self ! TryPersist2(seq)
      }
    case Persisted(key, id) =>
      if(cancellables.contains(id)) {
        val (replyTo) = waiting(id)
        replyTo ! SnapshotAck(key, id)
        cleanup(id)
      }

    case TimedOut(id) =>
      if(cancellables.contains(id)) {
        val replyTo = waiting(id)
        replyTo ! OperationFailed(id)
        cleanup(id)
      }

    case TryPersist2(id) =>
      if(cancellables.contains(id)) {
        persistence ! toPersist(id)
        context.system.scheduler.scheduleOnce(100.millis, self, TryPersist2(id))
      }
  }

}

