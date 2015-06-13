package kvstore

import akka.actor.{Cancellable, Props, Actor, ActorRef}
import akka.util.Timeout
import kvstore.Persistence.{Persisted, Persist}
import scala.concurrent.duration._

object Replicator {
  case class Replicate(key: String, valueOption: Option[String], id: Long)
  case class Replicated(key: String, id: Long)
  
  case class Snapshot(key: String, valueOption: Option[String], seq: Long)
  case class SnapshotAck(key: String, seq: Long)
  case object DoReplicate

  def props(replica: ActorRef): Props = Props(new Replicator(replica))
}

class Replicator(val replica: ActorRef) extends Actor {
  import Replicator._
  import Replica._
  import context.dispatcher
  
  /*
   * The contents of this actor is just a suggestion, you can implement it in any way you like.
   */

  // map from sequence number to pair of sender and request
  var acks = Map.empty[Long, (ActorRef, Replicate)]
  // a sequence of not-yet-sent snapshots (you can disregard this if not implementing batching)
  var pending = Vector.empty[Snapshot]
  
  var _seqCounter = 0L
  def nextSeq = {
    val ret = _seqCounter
    _seqCounter += 1
    ret
  }

  import akka.pattern._
  /* TODO Behavior for the Replicator. */

  var canc :Cancellable = null

  def receive: Receive = {
    case r@Replicate(key, valueOption, id) =>
      acks = acks + (nextSeq -> (sender() -> r))
      self ! DoReplicate

    case DoReplicate =>
      if(acks.nonEmpty) {
        println(s"Replicator ${self.path.name} sending Snapshot (there are ${acks.size} waiting.)")
        val seq = acks.keys.min
        val (replyTo, replicate) = acks(seq)
        implicit val timeout = Timeout(100.millis)
        canc = context.system.scheduler.scheduleOnce(100.millis, self, DoReplicate)
        replica ! Snapshot(replicate.key, replicate.valueOption, seq)
      }

    case SnapshotAck(key, seq) =>
      println(s"Replicator ${self.path.name} Received SnapshotAck")
      canc.cancel()

      acks.get(seq).foreach { case (replyTo, replicate) =>
        replyTo ! Replicated(key, replicate.id)
      }
      acks = acks - seq
  }

}
