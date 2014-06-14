package name.trofimov
package amqp

import net.liftweb.actor.LAFuture

package object rpc {

  import net.liftweb.json._

  type AmqpMessageFuture[T] = LAFuture[AmqpMessage[T]]

  type AmqpHeaders = Map[String, AnyRef]

  val AmqpNoHeaders: Map[String, AnyRef] = Map.empty

  class JsonApiRequest(val command: String, val data: Option[JValue] = None, val headers: AmqpHeaders = AmqpNoHeaders)

  class JsonApiResponse(val data: JValue, val headers: AmqpHeaders = AmqpNoHeaders)

}
