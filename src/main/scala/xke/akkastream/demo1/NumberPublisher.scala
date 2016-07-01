package xke.akkastream.demo1

import java.math.BigInteger

import akka.actor.{ActorLogging, Props}
import akka.stream.actor.ActorPublisher
import akka.stream.actor.ActorPublisherMessage.{Cancel, Request}

import scala.concurrent.duration.FiniteDuration

object NumberPublisher {

  def props(delay: FiniteDuration) = Props(new NumberPublisher(delay))
}

class NumberPublisher(delay: FiniteDuration) extends ActorPublisher[BigInteger] with ActorLogging {
  var prev = BigInteger.ZERO
  var curr = BigInteger.ZERO

  def receive = {
    case Request(count) =>
      log.info(s"Received Request ($count) from Subscriber")
      publishAsPossible()

    case Cancel =>
      log.info("Cancel Message Received -- Stopping")
      context.stop(self)

    case _ =>
  }

  def publishAsPossible() {
    while(isActive && totalDemand > 0) {
      onNext(getNextNumber())
    }
  }

  def getNextNumber(): BigInteger = {
    // Compute Fibonacci number
    if(curr == BigInteger.ZERO) {
      curr = BigInteger.ONE
    } else {
      val tmp = prev.add(curr)
      prev = curr
      curr = tmp
    }

    // Artificial delay
    Thread.sleep(delay.toMillis)
    curr
  }
}
