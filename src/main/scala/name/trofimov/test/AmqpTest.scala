package name.trofimov.test

import net.liftweb.actor._
import net.liftweb.common._
import intenium.online.amqp.rpc._
import net.liftweb.json._

object AmqpTest {

  object CF extends RpcConnectionFactory {
    import com.rabbitmq.client.ConnectionFactory._
    setHost("localhost")
    setUsername(DEFAULT_USER)
    setPassword(DEFAULT_PASS)
  }

  abstract class Server extends JsonApiServer(CF) {
    def serverInstance: String

    override def produceResponseToRequest(request: JsonApiRequest): Box[JsonApiResponse] = {
      import JsonDSL._
      request.command match {
        case "hello" => Full(new JsonApiResponse(("message" -> request.data.map(_ \ "message") ) ~ ("result" -> "OK")))
        case unknown => Failure(s"Unknown command <$unknown>")
      }
    }

    logger.debug(s"Start RTC Server $serverInstance")
    override def rpcServerProductName = "bonga"
    override def rpcServerServiceName = "service"
  }

  val Client = new JsonApiClient(CF) {
    override def rpcServerProductName = "bonga"
    override def rpcServerServiceName = "service"
  }

  val Server1 = new Server {
    def serverInstance = "Server1"
  }

  val Server2 = new Server {
    def serverInstance = "Server2"
  }

  val Server3 = new Server {
    def serverInstance = "Server3"
  }

  val Server4 = new Server {
    def serverInstance = "Server4"
  }

  def shutdown(): Unit = {
    Client.shutdown()
    List(Server1, Server2, Server3, Server4) foreach(_.shutdown())
  }

  def sayHello(): Box[String] = {
    def testRequest(message: String): JsonApiRequest = {
      new JsonApiRequest("hello", Some(JObject(JField("message", JString(message)) :: Nil)))
    }
    val replyFuture = Client.sendRequest(new JsonApiRequest("hi"))
    val replyFuture2 = Client.sendRequest(testRequest("HELLO2"))
    val replyFuture3 = Client.sendRequest(testRequest("HELLO3"))
    val replyFuture4 = Client.sendRequest(testRequest("HELLO4"))
    val replyFuture5 = Client.sendRequest(testRequest("HELLO5"))
    val res: LAFuture[String] = for {
      m <- replyFuture
      m2 <- replyFuture2
      m3 <- replyFuture3
      m4 <- replyFuture4
      m5 <- replyFuture5
    } yield {
      s"""m="${m.map(_.data)}" m5="${m5.map(_.data)}" """
    }

    res.get(5000)
  }

}
