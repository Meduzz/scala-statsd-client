/*
 * Copyright {yyyy} {name of copyright owner}
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

package local.test

import java.net.InetSocketAddress

import akka.actor.ActorSystem
import se.chimps.metrics.client.{StatsDClient, Client}

import scala.util.{Try, Random}

object StatsGenerator extends App {
  implicit val system = ActorSystem("StatsGenerator")
  val socket = new InetSocketAddress("192.168.235.20", 8125)
  val client:StatsDClient = Client(Some(socket), Some(128))
  var i = 0

  while (true) {
    val start = System.currentTimeMillis()
    client.increment("counter", 5)
    client.gauge("guage", Random.nextInt(1000))
    client.set("histogram", i.toString)
    client.decrement("counter", 3)
    val end = System.currentTimeMillis()
    client.timing("timing", end-start)
    Try(Thread.sleep(1000L))
    i = i + 1
    println("slept 1 sec.")
  }
}
