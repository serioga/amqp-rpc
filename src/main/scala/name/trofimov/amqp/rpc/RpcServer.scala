package name.trofimov
package amqp.rpc

import com.rabbitmq.client._
import java.util.concurrent.ExecutorService
import net.liftweb.common._
import com.rabbitmq.client.AMQP.BasicProperties
import net.liftweb.util.Helpers._
import scala.collection.JavaConverters._
import intenium.online.util.Execute

private case class RpcServerIncomingRequest[T](message: AmqpMessage[T], correlationId: String, replyToQueue: String)

private case class RpcServerOutgoingResponse[T](message: AmqpMessage[T], correlationId: String, replyToQueue: String)

abstract class RpcServer[T](cf: RpcConnectionFactory)(implicit executor: ExecutorService = null) extends RpcServerId with Loggable {

  client =>

  /**
   * Calculate response to incoming request.
   * @param message Incoming message to prepare response for.
   * @return Outgoing response message.
   */
  def responseToAmqpMessage(message: AmqpMessage[T]): AmqpMessage[T]

  def shutdown(): Unit = actor.shutdown()

  protected val actor = new RpcClientServerActor(cf)(executor) {

    actor =>

    /**
     * Override this to configure the Channel and Consumer.
     */
    protected def configure(channel: Channel): Unit = {
      // NOTE: Queue expiration, message TTL are managed via broker admin.
      channel.queueDeclare(rpcRequestQueue, false, false, false, null)
      channel.basicConsume(rpcRequestQueue, false, new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) = {
          val deliveryTag = envelope.getDeliveryTag
          val correlationId = properties.getCorrelationId
          val replyQueue = properties.getReplyTo
          actor ! RpcServerIncomingRequest(AmqpMessage(body, properties), correlationId, replyQueue)
          channel.basicAck(deliveryTag, false)
        }
      })
    }

    protected def sendResponseMessage(response: RpcServerOutgoingResponse[T]) {
      val props = new BasicProperties.Builder().correlationId(response.correlationId).headers(response.message.headers.asJava).build()
      def publishDescription = s"RpcServer: sendResponseMessage($response)"
      tryo(channel.basicPublish(rpcExchangeName, response.replyToQueue, props, response.message.rawBody)) match {
        case Full(_) => logger.debug(publishDescription)
        case e: EmptyBox => logger.error((e ?~! publishDescription).messageChain)
      }
    }

    override protected def messageHandler = super.messageHandler orElse {
      case request: RpcServerIncomingRequest[T] => {
        logger.debug(s"RpcServer: Receive request $request")
        Execute(() => {
          actor ! RpcServerOutgoingResponse(responseToAmqpMessage(request.message), request.correlationId, request.replyToQueue)
        })
      }
      case response: RpcServerOutgoingResponse[T] => {
        sendResponseMessage(response)
      }
    }
  }

}
