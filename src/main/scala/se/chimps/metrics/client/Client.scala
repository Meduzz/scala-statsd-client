/*
 * Copyright 2014 Meduzz
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package se.chimps.metrics.client

import java.net.InetSocketAddress
import java.nio.ByteBuffer

import akka.actor._
import akka.io.{UdpConnected, IO}
import akka.util.{ByteStringBuilder, ByteString}

/**
 * Statsd client companion object.
 */
object Client {

  val ACTOR_NAME = "UdpActor"
  val SYSTEM_NAME = "statsd"

  def apply(host: Option[InetSocketAddress], bufferSize: Option[Int]): StatsDClient = {
    val system = ActorSystem(Client.SYSTEM_NAME)
    new StatsDClient(actor(host, bufferSize, system))
  }

  def apply(host: Option[String], port: Option[Int], bufferSize: Option[Int]): StatsDClient = {
    val system = ActorSystem(Client.SYSTEM_NAME)
    new StatsDClient(actor(socket(host, port), bufferSize, system))
  }

  def apply(host: Option[InetSocketAddress], bufferSize: Option[Int])(implicit system: ActorSystem): ActorRef = {
    actor(host, bufferSize, system)
  }

  private def actor(host: Option[InetSocketAddress], bufferSize: Option[Int], system: ActorSystem): ActorRef = {
    system.actorOf(Props(classOf[UdpActor], host.getOrElse(socket(None, None)), bufferSize.getOrElse(512)), Client.ACTOR_NAME)
  }

  private def socket(host: Option[String], port: Option[Int]): Option[InetSocketAddress] = {
    Some(InetSocketAddress.createUnresolved(host.getOrElse("localhost"), port.getOrElse(8125)))
  }
}

/**
 * Statsd client.
 */
class StatsDClient(val connection: ActorRef) extends StatsdHelper {
}

class UdpActor(val host: InetSocketAddress, val bufferSize: Int) extends Actor with Stash {
  protected var connection: ActorRef = _
  // our udp connection to be.
  protected val buffer: ByteStringBuilder = ByteString.newBuilder
  // a buffer.
  protected val NL = "\n".getBytes("utf-8") // a new line, used in the protocol so separate multiple metrics.

  implicit val system = context.system

  // akka-io with "connected" udp.
  IO(UdpConnected) ! UdpConnected.Connect(self, host)

  override def receive: Receive = {
    case s:UdpConnected.Connected => {
      connection = sender()
    }
    case Metric(value: String) => bufferAndSend(value.getBytes("utf-8"))
    case dc@UdpConnected.Disconnect => {
      flush() // clean out the buffer.
      connection ! dc // then dc.
    }
    case UdpConnected.Disconnected => context.stop(self)
    case f:Flush => flush()
  }

  protected def bufferAndSend(metric: Array[Byte]) = {
    if (buffer.length + metric.length + NL.length > bufferSize) {
      // not enough space left in buffer, flush.
      flush()
    } else if (buffer.length > 0) {
      // add a new line.
      buffer.append(ByteString(NL))
    }

    // add the metric data.
    buffer.putBytes(metric)
  }

  protected def flush() = {
    // ignore flush if buffer is empty.
    if (buffer.length > 0) {
      connection ! UdpConnected.Send(buffer.result())

      buffer.clear()
    }
  }
}


/*
 * Metrics type classes.
 */

object Metric {
  // so we can pattern match the value of the metric without sweat.
  def unapply(value: Metric): Option[String] = Some(value.toString)
}

/**
 * Base trait for metrics, defining key, value & sampleRate.
 * Also forces implementations to define a typ (metric type)
 */
trait Metric {
  def key: String

  def value: String

  def sampleRate: Double

  protected def typ: String

  override def toString: String = {
    val sample = if (sampleRate < 1) {
      s"|@${sampleRate}"
    } else {
      ""
    }
    s"${key}:${value}|${typ}${sample}"
  }
}

class Timing(val key: String, val theValue: Long, val sampleRate: Double = 1.0) extends Metric {
  override protected val typ: String = "ms"

  override def value: String = theValue.toString
}

class Counter(val key: String, val theValue: Int, val sampleRate: Double = 1.0) extends Metric {
  override protected val typ: String = "c"

  override def value: String = theValue.toString
}

class Gauge(val key: String, val theValue: Int, val sampleRate: Double = 1.0) extends Metric {
  override protected val typ: String = "g"

  override def value: String = theValue.toString
}

class Set(val key: String, val value: String, val sampleRate: Double = 1.0) extends Metric {
  override protected val typ: String = "s"
}

/*
 * Utils
 */

/**
 * Helper methods packaged as a trait, so it can be used from your actors or the Statsd class :)
 */
trait StatsdHelper {
  def connection: ActorRef // this should be an UdpActor tbh.

  def timing(key: String, value: Long, sampleRate: Double = 1.0) = connection ! new Timing(key, value, sampleRate)

  def increment(key: String, magnitude: Int = 1, sampleRate: Double = 1.0) = connection ! new Counter(key, magnitude, sampleRate)

  def decrement(key: String, magnitude: Int = 1, sampleRate: Double = 1.0) = connection ! new Counter(key, magnitude * -1, sampleRate)

  def gauge(key: String, value: Int, sampleRate: Double = 1.0) = connection ! new Gauge(key, value, sampleRate)

  def set(key: String, value: String, sampleRate: Double = 1.0) = connection ! new Set(key, value, sampleRate)
}

case class Flush()