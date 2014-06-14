package name.trofimov
package amqp.rpc

import com.rabbitmq.client._
import java.util.concurrent.ExecutorService
import net.liftweb.common._
import net.liftweb.util.Helpers._
import com.rabbitmq.client.AMQP.BasicProperties
import java.util.UUID
import scala.collection.JavaConverters._
import intenium.online.util.Execute


private case class RpcClientOutgoingRequest[T](message: AmqpMessage[T], replyFuture: AmqpMessageFuture[T])

private case class RpcClientIncomingResponse[T](message: AmqpMessage[T], correlationId: String)

private case object RpcClientCleanUp

/**
 * Client-side actor for RPC calls.
 * @param cf Connection factory.
 * @param executor Executor service.
 * @tparam T The type of AMQP message body.
 */
abstract class RpcClient[T](cf: RpcConnectionFactory)(implicit executor: ExecutorService = null) extends RpcServerId with Loggable {

  client =>

  def sendAmqpMessage(message: AmqpMessage[T]): AmqpMessageFuture[T] = {
    import intenium.online.util.SideEffects._
    new AmqpMessageFuture[T] ~ {
      actor ! RpcClientOutgoingRequest(message, _)
    }
  }

//  /**
//   * Convert incoming response to result type..
//   * @param message The message to convert.
//   * @return Result with new type..
//   */
//  def convertResponseToResult(message: AmqpMessage[T]): R

  def shutdown(): Unit = actor.shutdown()

  val waitResponseMillis = 30000

  val rpcResponseQueue = rpcServerIdPrefix(s"rpc-response-$millis-${randomString(4)}")

  protected val actor = new RpcClientServerActor(cf)(executor) {

    actor =>

    /**
     * Override this to configure the Channel and Consumer.
     */
    protected def configure(channel: Channel): Unit = {
      // NOTE: Queue should not be exclusive otherwise we can get an exception when connection lost-restored.
      channel.queueDeclare(rpcResponseQueue, false, false, true, null)
      channel.basicConsume(rpcResponseQueue, true, new DefaultConsumer(channel) {
        override def handleDelivery(consumerTag: String, envelope: Envelope, properties: BasicProperties, body: Array[Byte]) = {
          val correlationId = properties.getCorrelationId
          actor ! RpcClientIncomingResponse(AmqpMessage(body, properties), correlationId)
        }
      })
    }

    /**
     * Store info about requests in progress.
     * This is a map of UUID -> (Result future, Expiration millis).
     * Expiration millis are used to remove non-answered requests from the storage.
     */
    private val activeRequestMap: scala.collection.mutable.Map[String, (AmqpMessageFuture[T], Long)] = scala.collection.mutable.Map.empty

    protected def sendRequestMessage(request: RpcClientOutgoingRequest[T]) {
      val correlationId = UUID.randomUUID.toString
      // Expiration time is managed in AMQP broker admin
      val props = new BasicProperties.Builder().correlationId(correlationId).replyTo(rpcResponseQueue).headers(request.message.headers.asJava).build()
      def publishDescription = s"RpcClient: Send message $correlationId to $rpcRequestQueue: ${request.message}"
      tryo(channel.basicPublish(rpcExchangeName, rpcRequestQueue, props, request.message.rawBody)) match {
        case Full(_) => {
          logger.debug(publishDescription)
          activeRequestMap.put(correlationId, (request.replyFuture, millis + waitResponseMillis))
        }
        case e: EmptyBox => logger.error((e ?~! publishDescription).messageChain)
      }
    }

//    /**
//     * Check if specific message is RPC message with valid data.
//     * @param message The message to validate.
//     * @return Full or empty box.
//     */
//    def convertIncomingResponse(message: AmqpMessage[T]): R = client.convertResponseToResult(message)

    private def scheduleCleanUp(): Unit = Execute(() => actor ! RpcClientCleanUp, waitResponseMillis)

    override def beforeShutdown() = {
      actor ! RpcClientCleanUp
    }

    override protected def messageHandler = super.messageHandler orElse {
      case request: RpcClientOutgoingRequest[T] => {
        sendRequestMessage(request)
      }
      case response: RpcClientIncomingResponse[T] => {
        activeRequestMap.remove(response.correlationId).foreach(request => {
          logger.debug(s"Receive response $response")
          // convertIncomingResponse is potentially long operation, satisfy result in background
          Execute(() => request._1.satisfy(response.message))
        })
      }
      case RpcClientCleanUp => {
        val expirationLimit = millis
        val toRemove = activeRequestMap collect {
          case elem@(uuid, (future, expire)) if expire < expirationLimit => {
            future.fail(Failure(s"Wait response time out"))
            uuid
          }
        }
        if (!toRemove.isEmpty) {
          logger.warn(s"Remove unanswered request info $toRemove")
          activeRequestMap --= toRemove
        }
        scheduleCleanUp()
      }
    }

    scheduleCleanUp()

  }
}
