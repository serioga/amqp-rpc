package name.trofimov
package amqp.rpc

import scala.language.implicitConversions
import net.liftweb.common._
import com.rabbitmq.client.AMQP.BasicProperties
import scala.collection.JavaConverters._

/**
 * The message passed via AMQP for RPC based API.
 * @param body The message body.
 * @param headers Optional message headers for additional data passed (i.e. binary attachments).
 * @tparam T The type of the message body.
 */
case class AmqpMessage[T](body: T, headers: AmqpHeaders = AmqpNoHeaders) {
  /**
   * Binary representation of the message body.
   */
  def rawBody: Array[Byte] = {
    import java.io.{ObjectOutputStream, ByteArrayOutputStream}
    val bytes = new ByteArrayOutputStream
    val store = new ObjectOutputStream(bytes)
    store.writeObject(body)
    store.close()
    bytes.toByteArray
  }
}

case object AmqpMessage {
  /**
   * Instantiate RPC message by binary data received from AMQP broker.
   * @param rawBody The binary data to convert to message body.
   * @param properties The AMQP message properties to get headers from.
   * @tparam T The type of the message body.
   * @return New RpcMessage instance.
   */
  def apply[T](rawBody: Array[Byte], properties: BasicProperties): AmqpMessage[T] = {
    import java.io.{ByteArrayInputStream, ObjectInputStream}
    val headers = (Box !! properties.getHeaders).map(_.asScala.toMap) openOr Map.empty
    val in = new ObjectInputStream(new ByteArrayInputStream(rawBody))
    val body = in.readObject.asInstanceOf[T]
    new AmqpMessage(body, headers)
  }

  /**
   * Implicit conversion from some value to RpcMessage with this value as body, without headers.
   * @param body The value to use as message body.
   * @tparam T The type of the message body.
   * @return Corresponding RPC message.
   */
  implicit def fromTypeWithoutHeaders[T](body: T): AmqpMessage[T] = new AmqpMessage[T](body)
}

