package name.trofimov
package amqp.rpc

import com.rabbitmq.client.ConnectionFactory

class RpcConnectionFactory extends ConnectionFactory {

  /**
   * Set connection heartbeat to be short enough to avoid zombie RPC connections
   */
  setRequestedHeartbeat(5)
}
