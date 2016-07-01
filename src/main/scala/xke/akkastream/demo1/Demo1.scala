package xke.akkastream.demo1

import java.math.BigInteger

import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import akka.stream.{ActorMaterializer, ActorMaterializerSettings, Materializer}
import com.typesafe.config.ConfigFactory
import xke.akkastream.demo1.Demo1.Two

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._


// Basic stream
object Demo1_1 extends App {

  implicit val system = ActorSystem("demo1_1", ConfigFactory.empty())
  implicit val materializer: Materializer = ActorMaterializer()

  val source = Source.actorPublisher(NumberPublisher.props(50 millis))
  val sink = Sink.actorSubscriber(NumberSubscriber.props(100 millis))

  system.log.info("Running Stream")
  source.runWith(sink)
}

// Stream with intermediate operations
object Demo1_2 extends App {

  implicit val system = ActorSystem("demo1_2", ConfigFactory.empty())
  implicit val materializer: Materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withInputBuffer(16, 16)
  )

  val source = Source.actorPublisher[BigInteger](NumberPublisher.props(50 millis))
  val sink = Sink.actorSubscriber[BigInteger](NumberSubscriber.props(500 millis))

  system.log.info("Running Stream")
  source
    .map(_.multiply(Two))
    .map(_.multiply(Two))
    .runWith(sink)
}

// Stream with a slow blocking operation
object Demo1_3 extends App {

  implicit val system = ActorSystem("demo1_3", ConfigFactory.empty())
  implicit val materializer: Materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withInputBuffer(16, 16)
  )

  def longMultiply(input: BigInteger): BigInteger = {
    Thread.sleep((500 millis).toMillis)
    input.multiply(Two)
  }

  val source = Source.actorPublisher[BigInteger](NumberPublisher.props(50 millis))
  val sink = Sink.actorSubscriber[BigInteger](NumberSubscriber.props(100 millis))

  system.log.info("Running Stream")
  source
    .map(longMultiply)
    .runWith(sink)
}

// How to use mapAsync parallelism with an async operation
object Demo1_4 extends App {

  implicit val system = ActorSystem("demo1_4", ConfigFactory.empty())
  implicit val materializer: Materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withInputBuffer(16, 16)
  )
  implicit val executionContext: ExecutionContext = system.dispatcher

  def longMultiplyAsync(input: BigInteger): Future[BigInteger] = Future {
    Thread.sleep((500 millis).toMillis)
    input.multiply(Two)
  }

  val source = Source.actorPublisher[BigInteger](NumberPublisher.props(50 millis))
  val sink = Sink.actorSubscriber[BigInteger](NumberSubscriber.props(100 millis))

  val parallelism: Int = 4

  system.log.info("Running Stream")
  source
    .mapAsync(parallelism)(longMultiplyAsync)
    .runWith(sink)
}

// Slow sink : how the back-pressure and buffering work
object Demo1_5 extends App {

  implicit val system = ActorSystem("demo1_5", ConfigFactory.empty())
  implicit val materializer: Materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withInputBuffer(16, 16)
  )
  implicit val executionContext: ExecutionContext = system.dispatcher

  def longMultiplyAsync(input: BigInteger): Future[BigInteger] = Future {
    Thread.sleep((100 millis).toMillis)
    input.multiply(Two)
  }

  val source = Source.actorPublisher[BigInteger](NumberPublisher.props(50 millis))
  val sink = Sink.actorSubscriber[BigInteger](NumberSubscriber.props(500 millis))

  val parallelism: Int = 4

  system.log.info("Running Stream")
  source
    .mapAsync(parallelism)(longMultiplyAsync)
    .runWith(sink)
}

// More operations to process items by batch
object Demo1_6 extends App {

  implicit val system = ActorSystem("demo1_6", ConfigFactory.empty())
  implicit val materializer: Materializer = ActorMaterializer(
    ActorMaterializerSettings(system).withInputBuffer(32, 32)
  )
  implicit val executionContext: ExecutionContext = system.dispatcher

  def longBatchedMultiplyAsync(input: Seq[BigInteger]): Future[List[BigInteger]] = Future {
    Thread.sleep((4 seconds).toMillis)
    input.map(_.multiply(Two)).toList
  }

  val source = Source.actorPublisher[BigInteger](NumberPublisher.props(50 millis))
  val sink = Sink.actorSubscriber[BigInteger](NumberSubscriber.props(50 millis))

  val parallelism: Int = 4

  system.log.info("Running Stream")
  source
    .groupedWithin(10, 1 second)
    .mapAsync(parallelism)(longBatchedMultiplyAsync)
    .mapConcat(identity)
    .runWith(sink)
}

object Demo1 {
  val Two = BigInteger.valueOf(2)
}
