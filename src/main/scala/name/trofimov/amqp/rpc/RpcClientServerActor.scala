package name.trofimov
package amqp.rpc

import net.liftweb.actor._
import net.liftweb.common._
import com.rabbitmq.client._
import java.util.concurrent.ExecutorService
import intenium.online.util.Execute

private case class RpcChannelReconnect(retryInMillis: Long)

private case class RpcChannelConfigure(channel: Channel)

private case object RpcChannelShutdown

abstract class RpcClientServerActor(cf: RpcConnectionFactory)(implicit executor: ExecutorService = null) extends LiftActor with Loggable {

  actor =>

  protected def configure(channel: Channel): Unit

  protected def beforeShutdown(): Unit = ()

  protected var channel = connect()

  @volatile private var active = true

  def actorIsActive: Boolean = synchronized(active)

  def shutdown(): Unit = actor ! RpcChannelShutdown

  protected def withActiveActor(doSomething: => Unit): Unit = if (active) doSomething

  // NOTE: shutdownListener should be a function, otherwise it does not work!
  protected def shutdownListener = new ShutdownListener {
    override def shutdownCompleted(cause: ShutdownSignalException) = {
      withActiveActor {
        Execute(() => {
          actor ! RpcChannelReconnect(reconnectInMillis)
        }, reconnectInMillis)
      }
    }
  }

  protected def connect(): Channel = {
    val conn = cf.newConnection(executor)
    val channel = conn.createChannel()
    conn.addShutdownListener(shutdownListener)
    actor.insertMsgAtHeadOfQueue_!(RpcChannelConfigure(channel))
    channel
  }

  /**
   *
   * The name of the exchange used by AMQP-RPC communication.
   * Empty string means default exchange.
   */
  final val rpcExchangeName = ""

  /**
   * Period between reconnect attempts.
   */
  def reconnectInMillis: Long = 1000

  protected def messageHandler = {

    case RpcChannelConfigure(ch) =>
      configure(ch)

    case RpcChannelShutdown =>
      withActiveActor {
        logger.info(s"RpcClientServerActor: Shutdown connection to AMQP Server")
        active = false
        val conn = channel.getConnection
        beforeShutdown()
        conn.close()
      }

    case RpcChannelReconnect(retryInMillis) =>
      withActiveActor {
        try {
          channel = connect()
          logger.info(s"RpcClientServerActor: Successfully reconnected to AMQP Server")
        } catch {
          case e: Exception =>
            logger.info(s"RpcClientServerActor: Connect failed - ${e.getMessage}. Will attempt reconnect again in $retryInMillis ms.")
            Execute(() => {
              actor ! RpcChannelReconnect(retryInMillis)
            }, retryInMillis)
        }
      }
  }
}
