/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import akka.actor._
import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  case class Copy(elem:Int)

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._
  import BinaryTreeNode._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case c:Contains => root ! c
    case i:Insert => root ! i
    case r:Remove => root ! r
    case GC =>
      val newRoot = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))
      root ! CopyTo(newRoot)
      context.become(garbageCollecting(newRoot))
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case x:Operation => pendingQueue = pendingQueue :+ x
    case CopyFinished =>
      context.become(normal)
      root ! PoisonPill
      root = newRoot
      pendingQueue.foreach(self ! _)

  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._
  import BinaryTreeSet._
  import common._
  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  def createChild(pos:Position, insert:Insert) = {
    val child = context.actorOf(Props(classOf[BinaryTreeNode], insert.elem, false))
    subtrees = subtrees + (pos -> child)
    sendFinished(insert)
  }

  def sendFinished(oper: Operation) = {
    oper.requester ! OperationFinished(oper.id)
  }
  def sendContains(oper: Contains, contains:Boolean) = {
    oper.requester ! ContainsResult(oper.id, contains)
  }
  def doOrPassOn(pos:Position, ifNone: =>Unit, oper:Operation) = {
    subtrees.get(pos).fold(ifNone)(_ ! oper)
  }
  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case c@CopyTo(ref) =>
      context.become(copying(subtrees.values.toSet, false))
      if (!removed) {
        ref ! Insert(self, 0, elem)
      } else {
        self ! OperationFinished(0)
      }
      subtrees.values.foreach(_ ! c)
    case remove: Remove =>
      if(remove.elem < elem) {
        doOrPassOn(Left, sendFinished(remove), remove)
      } else if (remove.elem > elem) {
        doOrPassOn(Right, sendFinished(remove), remove)
      } else {
        removed = true
        sendFinished(remove)
      }
    case insert : Insert =>
      if(insert.elem < elem) {
        doOrPassOn(Left, createChild(Left, insert), insert)
      } else if (insert.elem > elem) {
        doOrPassOn(Right, createChild(Right, insert), insert)
      } else {
        removed = false
        sendFinished(insert)
      }
    case contains : Contains =>
      if(contains.elem < elem) {
        subtrees.get(Left).fold(sendContains(contains, false))(_ ! contains)
      } else if (contains.elem > elem) {
        subtrees.get(Right).fold(sendContains(contains, false))(_ ! contains)
      } else {
        sendContains(contains, !removed && elem == contains.elem)
      }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) =>
      context.become(copying(expected, true))
      maybeFinished(expected, true)
    case CopyFinished =>
      maybeFinished(expected, insertConfirmed)
      if(expected.nonEmpty) {
        context.become(copying(expected.tail, insertConfirmed))
      }
  }

  def maybeFinished(expected: Set[ActorRef], insertConfirmed: Boolean) = {
    if(expected.isEmpty && insertConfirmed) {
      context.parent ! CopyFinished
      subtrees.values.foreach(_ ! PoisonPill)
    }
  }

}
