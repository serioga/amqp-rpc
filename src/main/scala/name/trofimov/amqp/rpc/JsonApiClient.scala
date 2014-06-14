package name.trofimov
package amqp.rpc

import java.util.concurrent.ExecutorService
import net.liftweb.common._
import net.liftweb.json
import net.liftweb.json._
import net.liftweb.actor._

abstract class JsonApiClient(cf: RpcConnectionFactory)(implicit executor: ExecutorService = null) extends RpcClient[String](cf)(executor) {

  /**
   * Send RPC request and get response asynchronously.
   * @param request The API request to be sent.
   * @return The future with API response.
   */
  def sendRequest(request: JsonApiRequest): LAFuture[Box[JsonApiResponse]] = {
    sendAmqpMessage(convertRequestToAmqpMessage(request)) map convertAmqpMessageToResponseResult
  }

  /**
   * http://wiki.intenium-games.com/AMQP-RPC_based_API_ONLINE#RPC-Request_format
   */
  protected def convertRequestToAmqpMessage(request: JsonApiRequest): AmqpMessage[String] = {
    import net.liftweb.json.JsonDSL._
    val body: JValue =
      "rpc" -> {
        "request" -> {
          ("command" -> request.command) ~ ("data" -> request.data)
        }
      }
    AmqpMessage(json.compact(json.render(body)), request.headers)
  }

  /**
   * http://wiki.intenium-games.com/AMQP-RPC_based_API_ONLINE#RPC-Response_format
   */
  protected def convertAmqpMessageToResponseResult(message: AmqpMessage[String]):  Box[JsonApiResponse] = {
    def parseFailure(failure: JValue): Failure = {
      val failureMessage: Failure = failure \ "message" match {
        case JString(msg) => Failure(msg)
        case _ => Failure("")
      }
      failure \ "code" match {
        case JString(code) => failureMessage ~> code
        case JInt(code) => failureMessage ~> code
        case _ => failureMessage
      }
    }
    for {
      body <- (json.parseOpt(message.body): Box[JValue]) ?~! s"Parse JSON from ${message.body}"
      data <- {
        body \ "rpc" \ "response" match {
          case JNothing =>
            Failure(s"Invalid RPC-Response format in ${json.compact(json.render(body))}")
          case response => response \ "data" match {
            case JNothing => parseFailure(response \ "failure")
            case data => Full(data)
          }
        }
      }
    } yield new JsonApiResponse(data, message.headers)
  }
}
