package name.trofimov
package amqp.rpc

trait RpcServerId {

  def rpcServerProductName: String

  def rpcServerServiceName: String

  def rpcServerId = s"$rpcServerProductName.$rpcServerServiceName"

  def rpcServerIdPrefix(appendTo: String): String = rpcServerId + "_" + appendTo

  val rpcRequestQueue = rpcServerIdPrefix("rpc-request")

}
