package name.trofimov
package amqp.rpc

import java.util.concurrent.ExecutorService
import net.liftweb.common._
import net.liftweb.json
import net.liftweb.json._
import intenium.online.liftweb.common._

abstract class JsonApiServer(cf: RpcConnectionFactory)(implicit executor: ExecutorService = null) extends RpcServer[String](cf)(executor) {

  /**
   * Transform request to response
   */
  def produceResponseToRequest(request: JsonApiRequest): Box[JsonApiResponse]

  def responseToAmqpMessage(message: AmqpMessage[String]): AmqpMessage[String] = {
    val transform =
      convertAmqpMessageToRequest _ andThen
        (_ flatMap produceResponseToRequest) andThen
        convertResponseToAmqpMessage
    transform(message)
 }

  /**
   * http://wiki.intenium-games.com/AMQP-RPC_based_API_ONLINE#RPC-Request_format
   */
  protected def convertAmqpMessageToRequest(message: AmqpMessage[String]):  Box[JsonApiRequest] = {
    for {
      body <- (json.parseOpt(message.body): Box[JValue]) ?~! s"Parse JSON from ${message.body}"
      (command, data) <- body \ "rpc" \ "request" match {
        case JNothing => Failure(s"Invalid RPC-Response format in ${json.compact(json.render(body))}")
        case request => request \ "command" match {
          case JString(command) =>
            val data: Option[JValue] = request \ "data" match {
              case JNothing => None
              case d => Some(d)
            }
            Full((command, data))
          case _ => Failure(s"Missing 'command' in incoming request ${json.compact(json.render(body))}")
        }
      }
    } yield new JsonApiRequest(command, data, message.headers)
  }

  /**
   * http://wiki.intenium-games.com/AMQP-RPC_based_API_ONLINE#RPC-Response_format
   */
  protected def convertResponseToAmqpMessage(response: Box[JsonApiResponse]): AmqpMessage[String] = {
    import net.liftweb.json.JsonDSL._
    def formatResponse(responseBody: JValue): JValue = {
      "rpc" -> {
        "response" -> responseBody
      }
    }
    def formatResponseFailure(failure: EmptyBox, failureCode: Option[Any] = None): JValue = {
      val message: Option[String] = failure match {
        case f: Failure => Some(f.messageChain)
        case _ => None
      }
      val code: Option[JValue] = failureCode map {
        case num: Int => JInt(num)
        case num: Long => JInt(num)
        case str => JString(str.toString)
      }
      formatResponse(
        "failure" -> {
          ("message" -> message) ~ ("code" -> code)
        }
      )
    }
    val body: JValue = response match {
      case Full(r) => formatResponse("data" -> r.data)
      case e: EmptyBox => e match {
        case ParamFailureInChain(_, _, _, code) => formatResponseFailure(e, Some(code))
        case _ => formatResponseFailure(e)
      }
    }
    AmqpMessage(json.compact(json.render(body)), response.map(_.headers) openOr AmqpNoHeaders)
  }

}
