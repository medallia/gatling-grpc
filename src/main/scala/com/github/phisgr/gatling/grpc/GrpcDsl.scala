package com.github.phisgr.gatling.grpc

import com.github.phisgr.gatling.grpc.action.GrpcCallActionBuilder
import com.github.phisgr.gatling.grpc.protocol.GrpcProtocol
import io.gatling.commons.NotNothing
import io.gatling.commons.validation.{Failure, Success}
import io.gatling.core.session.Expression
import io.gatling.core.session.el.ElMessages
import io.grpc.stub.{AbstractStub, ClientCalls, StreamObserver}
import io.grpc.{CallOptions, Channel, ManagedChannelBuilder, MethodDescriptor}
import scalapb.grpc.Grpc.guavaFuture2ScalaFuture

import scala.concurrent.{Future, Promise}
import scala.reflect.ClassTag
import scala.util.Try


trait GrpcDsl {
  def grpc(channelBuilder: ManagedChannelBuilder[_]) = GrpcProtocol(channelBuilder)

  def grpc(requestName: Expression[String]) = new Call(requestName)

  class Call private[gatling](requestName: Expression[String]) {
    def service[Service <: AbstractStub[Service]](stub: Channel => Service) = new CallWithService(requestName, stub)

    def rpc[Req, Res](method: MethodDescriptor[Req, Res]): RpcCallBuilder[Req, Res] = {
      if (method.getType == MethodDescriptor.MethodType.UNARY) {
        new UnaryCallWithMethod(requestName, method)
      } else if (method.getType == MethodDescriptor.MethodType.SERVER_STREAMING) {
        new ServerStreamingCallWithMethod(requestName, method)
      } else {
        throw new AssertionError("Method type not supported " + method.getType)
      }
    }
  }

  trait RpcCallBuilder[Req, Res] {

    def payload(req: Expression[Req]): GrpcCallActionBuilder[Req, Res]

  }

  class UnaryCallWithMethod[Req, Res] private[gatling](requestName: Expression[String], method: MethodDescriptor[Req, Res]) extends RpcCallBuilder[Req, Res] {
    val f = { channel: Channel =>
      request: Req =>
        guavaFuture2ScalaFuture(ClientCalls.futureUnaryCall(channel.newCall(method, CallOptions.DEFAULT), request))
    }

    def payload(req: Expression[Req]) = GrpcCallActionBuilder(requestName, f, req, headers = Nil)
  }

  // TODO change response type to a collection of Res
  class MyStreamObserver[Res] extends StreamObserver[Res] {

    val promise: Promise[Res] = Promise[Res]()

    var lastRes: Option[Res] = Option.empty

    override def onNext(value: Res) {
      lastRes = Option.apply(value)
    }

    override def onError(t: Throwable) {
      t.printStackTrace()
      promise.failure(t)
    }

    override def onCompleted() {
      promise.complete(Try(lastRes.get))
    }

  }

  class ServerStreamingCallWithMethod[Req, Res] private[gatling](requestName: Expression[String], method: MethodDescriptor[Req, Res]) extends RpcCallBuilder[Req, Res] {
    val f = { channel: Channel =>
      request: Req => {
        val mso = new MyStreamObserver[Res]
        ClientCalls.asyncServerStreamingCall(channel.newCall(method, CallOptions.DEFAULT), request, mso)
        mso.promise.future
      }
    }

    def payload(req: Expression[Req]) = GrpcCallActionBuilder(requestName, f, req, headers = Nil)
  }


  class CallWithService[Service <: AbstractStub[Service]] private[gatling](requestName: Expression[String], stub: Channel => Service) {
    def rpc[Req, Res](fun: Service => Req => Future[Res])(request: Expression[Req]) =
      GrpcCallActionBuilder(requestName, stub andThen fun, request, headers = Nil)
  }

  def $[T: ClassTag : NotNothing](name: String): Expression[T] = s => s.attributes.get(name) match {
    case Some(t: T) => Success(t)
    case None => ElMessages.undefinedSessionAttribute(name)
    case Some(t) => Failure(s"Value $t is of type ${t.getClass.getName}, " +
      s"expected ${implicitly[ClassTag[T]].runtimeClass.getName}")
  }
}
