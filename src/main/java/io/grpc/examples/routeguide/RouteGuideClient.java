package io.grpc.examples.routeguide;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Tao Li on 2016/2/4.
 */
public class RouteGuideClient {
  private static final Logger logger = Logger.getLogger(RouteGuideClient.class.getName());

  private final ManagedChannel channel;
  private final RouteGuideGrpc.RouteGuideBlockingStub blockingStub;
  private final RouteGuideGrpc.RouteGuideStub asyncStub;

  public RouteGuideClient(String host, int port) {
    channel = ManagedChannelBuilder.forAddress(host, port).usePlaintext(true).build();
    blockingStub = RouteGuideGrpc.newBlockingStub(channel);
    asyncStub = RouteGuideGrpc.newStub(channel);
  }

  public void shutdown() throws InterruptedException {
    channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
  }

  public void getFeature(int lat, int lon) {
    logger.log(Level.INFO, "*** GetFeature: lat={0} lon={1}", new Object[]{lat, lon});

    Point request = Point.newBuilder().setLatitude(lat).setLongitude(lon).build();

    Feature feature = null;
    try {
      feature = blockingStub.getFeature(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
    }
    if (RouteGuideUtil.exists(feature)) {
      logger.log(Level.INFO, "Found feature called \"{0}\" at {1}, {2}", new Object[]{
          feature.getName(),
          RouteGuideUtil.getLatitude(feature.getLocation()),
          RouteGuideUtil.getLongitude(feature.getLocation())
      });
    } else {
      logger.log(Level.INFO, "Found no feature at {0}, {1}", new Object[]{
          RouteGuideUtil.getLatitude(feature.getLocation()),
          RouteGuideUtil.getLongitude(feature.getLocation())
      });
    }
  }

  public void listFeatures(int lowLat, int lowLon, int hiLat, int hiLon) {
    logger.log(Level.INFO, "*** ListFeatures: lowLat={0} lowLon={1} hiLat={2} hiLon={3}",
        new Object[]{lowLat, lowLon, hiLat, hiLon});

    Rectangle request = Rectangle.newBuilder()
        .setLo(Point.newBuilder().setLatitude(lowLat).setLongitude(lowLon).build())
        .setHi(Point.newBuilder().setLatitude(hiLat).setLongitude(hiLon).build()).build();
    Iterator<Feature> features;
    try {
      features = blockingStub.listFeatures(request);
    } catch (StatusRuntimeException e) {
      logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
      return;
    }

    StringBuilder responseLog = new StringBuilder("Result: ");
    while (features.hasNext()) {
      Feature feature = features.next();
      responseLog.append(feature);
    }
    logger.log(Level.INFO, responseLog.toString());
  }

  public void recordRoute(List<Feature> features, int numPoints) throws InterruptedException {
    logger.log(Level.INFO, "*** RecordRoute");
    final CountDownLatch finishLatch = new CountDownLatch(1);
    StreamObserver<RouteSummary> responseObserver = new StreamObserver<RouteSummary>() {
      @Override
      public void onNext(RouteSummary summary) {
        logger.log(Level.INFO, "Finished trip with {0} points. Passed {1} features. "
            + "Travelled {2} meters. It took {3} seconds.", new Object[]{
            summary.getPointCount(), summary.getFeatureCount(),
            summary.getDistance(), summary.getElapsedTime()});
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        logger.log(Level.WARNING, "RecoredRoute Failed: {0}", status);
        finishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        logger.log(Level.INFO, "Finish RecordRoute");
        finishLatch.countDown();
      }
    };

    StreamObserver<Point> requestObserver = asyncStub.recordRoute(responseObserver);
    try {
      Random rand = new Random();
      for (int i = 0; i < numPoints; i++) {
        int index = rand.nextInt(features.size());
        Point point = features.get(index).getLocation();
        logger.log(Level.INFO, "Visiting point {0}, {1}", new Object[]{
            RouteGuideUtil.getLatitude(point), RouteGuideUtil.getLongitude(point)
        });
        requestObserver.onNext(point);
        Thread.sleep(rand.nextInt(1000) + 500);
        if (finishLatch.getCount() == 0) {
          return;
        }
      }
    } catch (RuntimeException e) {
      requestObserver.onError(e);
      throw e;
    }
    requestObserver.onCompleted();
    finishLatch.await(1, TimeUnit.MINUTES);
  }

  public void routeChat() throws InterruptedException {
    logger.log(Level.INFO, "*** RouteChat");
    final CountDownLatch finishLatch = new CountDownLatch(1);
    StreamObserver<RouteNote> requestObserver = asyncStub.routeChat(new StreamObserver<RouteNote>() {
      @Override
      public void onNext(RouteNote note) {
        logger.log(Level.INFO, "Got message \"{0}\" at {1}, {2}", new Object[]{
            note.getMessage(), note.getLocation().getLatitude(), note.getLocation().getLongitude()
        });
      }

      @Override
      public void onError(Throwable t) {
        Status status = Status.fromThrowable(t);
        logger.log(Level.WARNING, "RouteChat Failed: {0}", status);
        finishLatch.countDown();
      }

      @Override
      public void onCompleted() {
        logger.log(Level.INFO, "Finished RouteChat");
        finishLatch.countDown();
      }
    });

    try {
      RouteNote[] requests = {
          RouteNote.newBuilder().setMessage("First Message")
              .setLocation(Point.newBuilder().setLatitude(0).setLongitude(0).build()).build(),
          RouteNote.newBuilder().setMessage("Second Message")
              .setLocation(Point.newBuilder().setLatitude(0).setLongitude(1).build()).build(),
          RouteNote.newBuilder().setMessage("Third Message")
              .setLocation(Point.newBuilder().setLatitude(1).setLongitude(0).build()).build(),
          RouteNote.newBuilder().setMessage("Fourth Message")
              .setLocation(Point.newBuilder().setLatitude(1).setLongitude(1).build()).build()
      };

      for (RouteNote request : requests) {
        logger.log(Level.INFO, "Sending message \"{0}\" at {1}, {2}", new Object[]{
            request.getMessage(), request.getLocation().getLatitude(), request.getLocation().getLongitude()
        });
        requestObserver.onNext(request);
      }
    } catch (RuntimeException e) {
      requestObserver.onError(e);
      throw e;
    }
    requestObserver.onCompleted();
    finishLatch.await(1, TimeUnit.MINUTES);
  }

  public static void main(String[] args) throws InterruptedException {
    List<Feature> features;
    try {
      features = RouteGuideUtil.parseFeatures(RouteGuideUtil.getDefaultFeaturesFile());
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    RouteGuideClient client = new RouteGuideClient("localhost", 8980);
    try {
      client.getFeature(409146138, -746188906);
      client.getFeature(0, 0);

      client.listFeatures(400000000, -750000000, 420000000, -730000000);

      client.recordRoute(features, 10);

      client.routeChat();
    } finally {
      client.shutdown();
    }
  }
}
