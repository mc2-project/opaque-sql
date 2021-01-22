package ra;

import static io.grpc.MethodDescriptor.generateFullMethodName;
import static io.grpc.stub.ClientCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ClientCalls.asyncClientStreamingCall;
import static io.grpc.stub.ClientCalls.asyncServerStreamingCall;
import static io.grpc.stub.ClientCalls.asyncUnaryCall;
import static io.grpc.stub.ClientCalls.blockingServerStreamingCall;
import static io.grpc.stub.ClientCalls.blockingUnaryCall;
import static io.grpc.stub.ClientCalls.futureUnaryCall;
import static io.grpc.stub.ServerCalls.asyncBidiStreamingCall;
import static io.grpc.stub.ServerCalls.asyncClientStreamingCall;
import static io.grpc.stub.ServerCalls.asyncServerStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnaryCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedStreamingCall;
import static io.grpc.stub.ServerCalls.asyncUnimplementedUnaryCall;

/**
 * <pre>
 * The greeting service definition.
 * </pre>
 */
@javax.annotation.Generated(
    value = "by gRPC proto compiler (version 1.34.1)",
    comments = "Source: src/main/protobuf/ra.proto")
public final class GreeterGrpc {

  private GreeterGrpc() {}

  public static final String SERVICE_NAME = "ra.Greeter";

  // Static method descriptors that strictly reflect the proto.
  private static volatile io.grpc.MethodDescriptor<ra.Ra.HelloRequest,
      ra.Ra.HelloReply> getSayHelloMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SayHello",
      requestType = ra.Ra.HelloRequest.class,
      responseType = ra.Ra.HelloReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<ra.Ra.HelloRequest,
      ra.Ra.HelloReply> getSayHelloMethod() {
    io.grpc.MethodDescriptor<ra.Ra.HelloRequest, ra.Ra.HelloReply> getSayHelloMethod;
    if ((getSayHelloMethod = GreeterGrpc.getSayHelloMethod) == null) {
      synchronized (GreeterGrpc.class) {
        if ((getSayHelloMethod = GreeterGrpc.getSayHelloMethod) == null) {
          GreeterGrpc.getSayHelloMethod = getSayHelloMethod =
              io.grpc.MethodDescriptor.<ra.Ra.HelloRequest, ra.Ra.HelloReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SayHello"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ra.Ra.HelloRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ra.Ra.HelloReply.getDefaultInstance()))
              .setSchemaDescriptor(new GreeterMethodDescriptorSupplier("SayHello"))
              .build();
        }
      }
    }
    return getSayHelloMethod;
  }

  private static volatile io.grpc.MethodDescriptor<ra.Ra.RARequest,
      ra.Ra.RAReply> getGetRAMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "GetRA",
      requestType = ra.Ra.RARequest.class,
      responseType = ra.Ra.RAReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<ra.Ra.RARequest,
      ra.Ra.RAReply> getGetRAMethod() {
    io.grpc.MethodDescriptor<ra.Ra.RARequest, ra.Ra.RAReply> getGetRAMethod;
    if ((getGetRAMethod = GreeterGrpc.getGetRAMethod) == null) {
      synchronized (GreeterGrpc.class) {
        if ((getGetRAMethod = GreeterGrpc.getGetRAMethod) == null) {
          GreeterGrpc.getGetRAMethod = getGetRAMethod =
              io.grpc.MethodDescriptor.<ra.Ra.RARequest, ra.Ra.RAReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "GetRA"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ra.Ra.RARequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ra.Ra.RAReply.getDefaultInstance()))
              .setSchemaDescriptor(new GreeterMethodDescriptorSupplier("GetRA"))
              .build();
        }
      }
    }
    return getGetRAMethod;
  }

  private static volatile io.grpc.MethodDescriptor<ra.Ra.KeyRequest,
      ra.Ra.KeyReply> getSendKeyMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendKey",
      requestType = ra.Ra.KeyRequest.class,
      responseType = ra.Ra.KeyReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<ra.Ra.KeyRequest,
      ra.Ra.KeyReply> getSendKeyMethod() {
    io.grpc.MethodDescriptor<ra.Ra.KeyRequest, ra.Ra.KeyReply> getSendKeyMethod;
    if ((getSendKeyMethod = GreeterGrpc.getSendKeyMethod) == null) {
      synchronized (GreeterGrpc.class) {
        if ((getSendKeyMethod = GreeterGrpc.getSendKeyMethod) == null) {
          GreeterGrpc.getSendKeyMethod = getSendKeyMethod =
              io.grpc.MethodDescriptor.<ra.Ra.KeyRequest, ra.Ra.KeyReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendKey"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ra.Ra.KeyRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ra.Ra.KeyReply.getDefaultInstance()))
              .setSchemaDescriptor(new GreeterMethodDescriptorSupplier("SendKey"))
              .build();
        }
      }
    }
    return getSendKeyMethod;
  }

  private static volatile io.grpc.MethodDescriptor<ra.Ra.QueryRequest,
      ra.Ra.QueryReply> getSendQueryMethod;

  @io.grpc.stub.annotations.RpcMethod(
      fullMethodName = SERVICE_NAME + '/' + "SendQuery",
      requestType = ra.Ra.QueryRequest.class,
      responseType = ra.Ra.QueryReply.class,
      methodType = io.grpc.MethodDescriptor.MethodType.UNARY)
  public static io.grpc.MethodDescriptor<ra.Ra.QueryRequest,
      ra.Ra.QueryReply> getSendQueryMethod() {
    io.grpc.MethodDescriptor<ra.Ra.QueryRequest, ra.Ra.QueryReply> getSendQueryMethod;
    if ((getSendQueryMethod = GreeterGrpc.getSendQueryMethod) == null) {
      synchronized (GreeterGrpc.class) {
        if ((getSendQueryMethod = GreeterGrpc.getSendQueryMethod) == null) {
          GreeterGrpc.getSendQueryMethod = getSendQueryMethod =
              io.grpc.MethodDescriptor.<ra.Ra.QueryRequest, ra.Ra.QueryReply>newBuilder()
              .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
              .setFullMethodName(generateFullMethodName(SERVICE_NAME, "SendQuery"))
              .setSampledToLocalTracing(true)
              .setRequestMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ra.Ra.QueryRequest.getDefaultInstance()))
              .setResponseMarshaller(io.grpc.protobuf.ProtoUtils.marshaller(
                  ra.Ra.QueryReply.getDefaultInstance()))
              .setSchemaDescriptor(new GreeterMethodDescriptorSupplier("SendQuery"))
              .build();
        }
      }
    }
    return getSendQueryMethod;
  }

  /**
   * Creates a new async stub that supports all call types for the service
   */
  public static GreeterStub newStub(io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GreeterStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GreeterStub>() {
        @java.lang.Override
        public GreeterStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GreeterStub(channel, callOptions);
        }
      };
    return GreeterStub.newStub(factory, channel);
  }

  /**
   * Creates a new blocking-style stub that supports unary and streaming output calls on the service
   */
  public static GreeterBlockingStub newBlockingStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GreeterBlockingStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GreeterBlockingStub>() {
        @java.lang.Override
        public GreeterBlockingStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GreeterBlockingStub(channel, callOptions);
        }
      };
    return GreeterBlockingStub.newStub(factory, channel);
  }

  /**
   * Creates a new ListenableFuture-style stub that supports unary calls on the service
   */
  public static GreeterFutureStub newFutureStub(
      io.grpc.Channel channel) {
    io.grpc.stub.AbstractStub.StubFactory<GreeterFutureStub> factory =
      new io.grpc.stub.AbstractStub.StubFactory<GreeterFutureStub>() {
        @java.lang.Override
        public GreeterFutureStub newStub(io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
          return new GreeterFutureStub(channel, callOptions);
        }
      };
    return GreeterFutureStub.newStub(factory, channel);
  }

  /**
   * <pre>
   * The greeting service definition.
   * </pre>
   */
  public static abstract class GreeterImplBase implements io.grpc.BindableService {

    /**
     * <pre>
     * Sends a greeting
     * </pre>
     */
    public void sayHello(ra.Ra.HelloRequest request,
        io.grpc.stub.StreamObserver<ra.Ra.HelloReply> responseObserver) {
      asyncUnimplementedUnaryCall(getSayHelloMethod(), responseObserver);
    }

    /**
     */
    public void getRA(ra.Ra.RARequest request,
        io.grpc.stub.StreamObserver<ra.Ra.RAReply> responseObserver) {
      asyncUnimplementedUnaryCall(getGetRAMethod(), responseObserver);
    }

    /**
     */
    public void sendKey(ra.Ra.KeyRequest request,
        io.grpc.stub.StreamObserver<ra.Ra.KeyReply> responseObserver) {
      asyncUnimplementedUnaryCall(getSendKeyMethod(), responseObserver);
    }

    /**
     */
    public void sendQuery(ra.Ra.QueryRequest request,
        io.grpc.stub.StreamObserver<ra.Ra.QueryReply> responseObserver) {
      asyncUnimplementedUnaryCall(getSendQueryMethod(), responseObserver);
    }

    @java.lang.Override public final io.grpc.ServerServiceDefinition bindService() {
      return io.grpc.ServerServiceDefinition.builder(getServiceDescriptor())
          .addMethod(
            getSayHelloMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                ra.Ra.HelloRequest,
                ra.Ra.HelloReply>(
                  this, METHODID_SAY_HELLO)))
          .addMethod(
            getGetRAMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                ra.Ra.RARequest,
                ra.Ra.RAReply>(
                  this, METHODID_GET_RA)))
          .addMethod(
            getSendKeyMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                ra.Ra.KeyRequest,
                ra.Ra.KeyReply>(
                  this, METHODID_SEND_KEY)))
          .addMethod(
            getSendQueryMethod(),
            asyncUnaryCall(
              new MethodHandlers<
                ra.Ra.QueryRequest,
                ra.Ra.QueryReply>(
                  this, METHODID_SEND_QUERY)))
          .build();
    }
  }

  /**
   * <pre>
   * The greeting service definition.
   * </pre>
   */
  public static final class GreeterStub extends io.grpc.stub.AbstractAsyncStub<GreeterStub> {
    private GreeterStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GreeterStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GreeterStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a greeting
     * </pre>
     */
    public void sayHello(ra.Ra.HelloRequest request,
        io.grpc.stub.StreamObserver<ra.Ra.HelloReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSayHelloMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void getRA(ra.Ra.RARequest request,
        io.grpc.stub.StreamObserver<ra.Ra.RAReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getGetRAMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sendKey(ra.Ra.KeyRequest request,
        io.grpc.stub.StreamObserver<ra.Ra.KeyReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSendKeyMethod(), getCallOptions()), request, responseObserver);
    }

    /**
     */
    public void sendQuery(ra.Ra.QueryRequest request,
        io.grpc.stub.StreamObserver<ra.Ra.QueryReply> responseObserver) {
      asyncUnaryCall(
          getChannel().newCall(getSendQueryMethod(), getCallOptions()), request, responseObserver);
    }
  }

  /**
   * <pre>
   * The greeting service definition.
   * </pre>
   */
  public static final class GreeterBlockingStub extends io.grpc.stub.AbstractBlockingStub<GreeterBlockingStub> {
    private GreeterBlockingStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GreeterBlockingStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GreeterBlockingStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a greeting
     * </pre>
     */
    public ra.Ra.HelloReply sayHello(ra.Ra.HelloRequest request) {
      return blockingUnaryCall(
          getChannel(), getSayHelloMethod(), getCallOptions(), request);
    }

    /**
     */
    public ra.Ra.RAReply getRA(ra.Ra.RARequest request) {
      return blockingUnaryCall(
          getChannel(), getGetRAMethod(), getCallOptions(), request);
    }

    /**
     */
    public ra.Ra.KeyReply sendKey(ra.Ra.KeyRequest request) {
      return blockingUnaryCall(
          getChannel(), getSendKeyMethod(), getCallOptions(), request);
    }

    /**
     */
    public ra.Ra.QueryReply sendQuery(ra.Ra.QueryRequest request) {
      return blockingUnaryCall(
          getChannel(), getSendQueryMethod(), getCallOptions(), request);
    }
  }

  /**
   * <pre>
   * The greeting service definition.
   * </pre>
   */
  public static final class GreeterFutureStub extends io.grpc.stub.AbstractFutureStub<GreeterFutureStub> {
    private GreeterFutureStub(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      super(channel, callOptions);
    }

    @java.lang.Override
    protected GreeterFutureStub build(
        io.grpc.Channel channel, io.grpc.CallOptions callOptions) {
      return new GreeterFutureStub(channel, callOptions);
    }

    /**
     * <pre>
     * Sends a greeting
     * </pre>
     */
    public com.google.common.util.concurrent.ListenableFuture<ra.Ra.HelloReply> sayHello(
        ra.Ra.HelloRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSayHelloMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<ra.Ra.RAReply> getRA(
        ra.Ra.RARequest request) {
      return futureUnaryCall(
          getChannel().newCall(getGetRAMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<ra.Ra.KeyReply> sendKey(
        ra.Ra.KeyRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSendKeyMethod(), getCallOptions()), request);
    }

    /**
     */
    public com.google.common.util.concurrent.ListenableFuture<ra.Ra.QueryReply> sendQuery(
        ra.Ra.QueryRequest request) {
      return futureUnaryCall(
          getChannel().newCall(getSendQueryMethod(), getCallOptions()), request);
    }
  }

  private static final int METHODID_SAY_HELLO = 0;
  private static final int METHODID_GET_RA = 1;
  private static final int METHODID_SEND_KEY = 2;
  private static final int METHODID_SEND_QUERY = 3;

  private static final class MethodHandlers<Req, Resp> implements
      io.grpc.stub.ServerCalls.UnaryMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ServerStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.ClientStreamingMethod<Req, Resp>,
      io.grpc.stub.ServerCalls.BidiStreamingMethod<Req, Resp> {
    private final GreeterImplBase serviceImpl;
    private final int methodId;

    MethodHandlers(GreeterImplBase serviceImpl, int methodId) {
      this.serviceImpl = serviceImpl;
      this.methodId = methodId;
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public void invoke(Req request, io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        case METHODID_SAY_HELLO:
          serviceImpl.sayHello((ra.Ra.HelloRequest) request,
              (io.grpc.stub.StreamObserver<ra.Ra.HelloReply>) responseObserver);
          break;
        case METHODID_GET_RA:
          serviceImpl.getRA((ra.Ra.RARequest) request,
              (io.grpc.stub.StreamObserver<ra.Ra.RAReply>) responseObserver);
          break;
        case METHODID_SEND_KEY:
          serviceImpl.sendKey((ra.Ra.KeyRequest) request,
              (io.grpc.stub.StreamObserver<ra.Ra.KeyReply>) responseObserver);
          break;
        case METHODID_SEND_QUERY:
          serviceImpl.sendQuery((ra.Ra.QueryRequest) request,
              (io.grpc.stub.StreamObserver<ra.Ra.QueryReply>) responseObserver);
          break;
        default:
          throw new AssertionError();
      }
    }

    @java.lang.Override
    @java.lang.SuppressWarnings("unchecked")
    public io.grpc.stub.StreamObserver<Req> invoke(
        io.grpc.stub.StreamObserver<Resp> responseObserver) {
      switch (methodId) {
        default:
          throw new AssertionError();
      }
    }
  }

  private static abstract class GreeterBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoFileDescriptorSupplier, io.grpc.protobuf.ProtoServiceDescriptorSupplier {
    GreeterBaseDescriptorSupplier() {}

    @java.lang.Override
    public com.google.protobuf.Descriptors.FileDescriptor getFileDescriptor() {
      return ra.Ra.getDescriptor();
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.ServiceDescriptor getServiceDescriptor() {
      return getFileDescriptor().findServiceByName("Greeter");
    }
  }

  private static final class GreeterFileDescriptorSupplier
      extends GreeterBaseDescriptorSupplier {
    GreeterFileDescriptorSupplier() {}
  }

  private static final class GreeterMethodDescriptorSupplier
      extends GreeterBaseDescriptorSupplier
      implements io.grpc.protobuf.ProtoMethodDescriptorSupplier {
    private final String methodName;

    GreeterMethodDescriptorSupplier(String methodName) {
      this.methodName = methodName;
    }

    @java.lang.Override
    public com.google.protobuf.Descriptors.MethodDescriptor getMethodDescriptor() {
      return getServiceDescriptor().findMethodByName(methodName);
    }
  }

  private static volatile io.grpc.ServiceDescriptor serviceDescriptor;

  public static io.grpc.ServiceDescriptor getServiceDescriptor() {
    io.grpc.ServiceDescriptor result = serviceDescriptor;
    if (result == null) {
      synchronized (GreeterGrpc.class) {
        result = serviceDescriptor;
        if (result == null) {
          serviceDescriptor = result = io.grpc.ServiceDescriptor.newBuilder(SERVICE_NAME)
              .setSchemaDescriptor(new GreeterFileDescriptorSupplier())
              .addMethod(getSayHelloMethod())
              .addMethod(getGetRAMethod())
              .addMethod(getSendKeyMethod())
              .addMethod(getSendQueryMethod())
              .build();
        }
      }
    }
    return result;
  }
}
