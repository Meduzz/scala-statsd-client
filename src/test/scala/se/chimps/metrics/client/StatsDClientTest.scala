package se.chimps.metrics.client

import java.net.InetSocketAddress

import akka.actor._
import akka.io.{Udp, IO}
import akka.testkit.{TestProbe, TestKitBase}
import akka.util.ByteString
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class StatsDClientTest extends FunSuite with TestKitBase with BeforeAndAfterAll {
  implicit lazy val system = ActorSystem()

  val probe = TestProbe()
  val socket = new InetSocketAddress("localhost", 8125)
  val fakeServer = system.actorOf(Props(classOf[StatsDServer], probe.ref, socket), "server")
  val client:StatsDClient = Client(Some(socket), Some(128))

  val timing = new Timing("spam", 1234L, 0.1)
  val counter = new Counter("spam", 1234, 0.1)
  val gauge = new Gauge("spam", 1234, 0.1)
  val set = new Set("spam", "1234", 0.1)

  override protected def afterAll(): Unit = system.shutdown()

  def assertMetric(metric:Metric, value:String) = {
    assert(metric.toString.equals(value), s"Metric ${metric.toString} was not equal to ${value}")
  }

  test("metrics are converted to nice strings") {
    assertMetric(timing, "spam:1234|ms|@0.1")
    assertMetric(counter, "spam:1234|c|@0.1")
    assertMetric(gauge, "spam:1234|g|@0.1")
    assertMetric(set, "spam:1234|s|@0.1")
  }

  test("StatsD with helpermethods are sending correct metrics") {
    val statsd = new StatsDClient(probe.ref)

    statsd.timing("spam", 1234L, 0.1)
    statsd.increment("spam", 1234, 0.1)
    statsd.gauge("spam", 1234, 0.1)
    statsd.set("spam", "1234", 0.1)

    statsd.timing("spam", 1234L)
    statsd.increment("spam", 1234)
    statsd.gauge("spam", 1234)
    statsd.set("spam", "1234")

    assertMetric(probe.expectMsgClass(timing.getClass), "spam:1234|ms|@0.1")
    assertMetric(probe.expectMsgClass(counter.getClass), "spam:1234|c|@0.1")
    assertMetric(probe.expectMsgClass(gauge.getClass), "spam:1234|g|@0.1")
    assertMetric(probe.expectMsgClass(set.getClass), "spam:1234|s|@0.1")

    assertMetric(probe.expectMsgClass(timing.getClass), "spam:1234|ms")
    assertMetric(probe.expectMsgClass(counter.getClass), "spam:1234|c")
    assertMetric(probe.expectMsgClass(gauge.getClass), "spam:1234|g")
    assertMetric(probe.expectMsgClass(set.getClass), "spam:1234|s")
  }

  test("client flushes on Flush message") {
    client.gauge("spam", 123)
    client.connection ! new Flush()

    val msg = probe.expectMsgClass(classOf[String])
    assert(msg.equals("spam:123|g"), s"Message ${msg} did not equal spam:123|g")
  }

  test("client behaves and data reaches the fakeServer") {
    client.gauge("test", 100)
    client.increment("spam")
    client.decrement("spam")
    client.timing("time", 1234567L)
    client.set("set", "value")
    client.connection ! new Flush()

    probe.expectMsgAllOf("test:100|g", "spam:1|c", "spam:-1|c", "time:1234567|ms", "set:value|s")
  }
}

class StatsDServer(val actor:ActorRef, val socket:InetSocketAddress) extends Actor with Stash {
  private var connection:ActorRef = _

  @throws[Exception](classOf[Exception])
  override def preStart(): Unit = {
    implicit val system = context.system

    IO(Udp) ! Udp.Bind(self, socket)
  }

  override def receive:Receive = {
    case s:Udp.Bound => {
      connection = sender()
    }
    case Udp.Received(data:ByteString, _) => {
      // make utf8 string, split on new line, filter out empties and bomb the probe.
      data.utf8String.split("\n").filter(str => !str.isEmpty).foreach(str => actor ! str)
    }
    case Udp.Unbind  => connection ! Udp.Unbind
    case Udp.Unbound => context.stop(self)
  }
}