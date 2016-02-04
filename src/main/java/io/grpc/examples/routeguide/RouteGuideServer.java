package io.grpc.examples.routeguide;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by Tao Li on 2016/2/4.
 */
public class RouteGuideServer {
  private static final Logger logger = Logger.getLogger(RouteGuideServer.class.getName());

  private final int port;
  private final Collection<Feature> features;
  private Server server;

  public RouteGuideServer(int port) {
    this(port, RouteGuideUtil.getDefaultFeaturesFile());
  }

  public RouteGuideServer(int port, URL featureFile) {
    try {
      this.port = port;
      features = RouteGuideUtil.parseFeatures(featureFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void start() throws IOException {
    server = ServerBuilder.forPort(port)
        .addService(RouteGuideGrpc.bindService(new RouteGuideService(features)))
        .build().start();
    logger.info("Server started, listening on " + port);
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        System.err.println("*** shutting down gRPC server since JVM is shutting down");
        RouteGuideServer.this.stop();
      }
    });
  }

  public void stop() {
    if (server != null) {
      server.shutdown();
    }
  }

  public void blockUntilShutdown() throws InterruptedException {
    if (server != null) {
      server.awaitTermination();
    }
  }

  public static void main(String[] args) throws Exception {
    RouteGuideServer server = new RouteGuideServer(8980);
    server.start();
    server.blockUntilShutdown();
  }

  private static class RouteGuideService implements RouteGuideGrpc.RouteGuide {
    private final Collection<Feature> features;
    private final ConcurrentMap<Point, List<RouteNote>> routeNotes =
        new ConcurrentHashMap<>();

    RouteGuideService(Collection<Feature> features) {
      this.features = features;
    }

    @Override
    public void getFeature(Point request, StreamObserver<Feature> responseObserver) {
      responseObserver.onNext(checkFeature(request));
      responseObserver.onCompleted();
    }

    @Override
    public void listFeatures(Rectangle request, StreamObserver<Feature> responseObserver) {
      int left = Math.min(request.getLo().getLongitude(), request.getHi().getLongitude());
      int right = Math.max(request.getLo().getLongitude(), request.getHi().getLongitude());
      int top = Math.max(request.getLo().getLatitude(), request.getHi().getLatitude());
      int bottom = Math.min(request.getLo().getLatitude(), request.getHi().getLatitude());

      for (Feature feature : features) {
        if (!RouteGuideUtil.exists(feature)) {
          continue;
        }

        int lat = feature.getLocation().getLatitude();
        int lon = feature.getLocation().getLongitude();
        if (lon >= left && lon <= right && lat >= bottom && lat <= top) {
          responseObserver.onNext(feature);
        }
      }
      responseObserver.onCompleted();
    }

    @Override
    public StreamObserver<Point> recordRoute(final StreamObserver<RouteSummary> responseObserver) {
      return new StreamObserver<Point>() {
        int pointCount;
        int featureCount;
        int distance;
        Point previous;
        long startTime = System.nanoTime();

        @Override
        public void onNext(Point point) {
          pointCount++;
          if (RouteGuideUtil.exists(checkFeature(point))) {
            featureCount++;
          }
          if (previous != null) {
            distance += calcDistance(previous, point);
          }
          previous = point;
        }

        @Override
        public void onError(Throwable throwable) {
          logger.log(Level.WARNING, "recordRoute cancelled");
        }

        @Override
        public void onCompleted() {
          long seconds = TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime);
          responseObserver.onNext(RouteSummary.newBuilder()
              .setPointCount(pointCount)
              .setFeatureCount(featureCount)
              .setDistance(distance)
              .setElapsedTime((int) seconds).build());
          responseObserver.onCompleted();
        }
      };
    }

    @Override
    public StreamObserver<RouteNote> routeChat(final StreamObserver<RouteNote> responseObserver) {
      return new StreamObserver<RouteNote>() {
        @Override
        public void onNext(RouteNote note) {
          List<RouteNote> notes = getOrCreateNotes(note.getLocation());

          for (RouteNote preNote : notes.toArray(new RouteNote[0])) {
            responseObserver.onNext(preNote);
          }

          notes.add(note);
        }

        @Override
        public void onError(Throwable t) {
          logger.log(Level.WARNING, "routeChat cancelled");
        }

        @Override
        public void onCompleted() {
          responseObserver.onCompleted();
        }
      };
    }

    private List<RouteNote> getOrCreateNotes(Point location) {
      List<RouteNote> notes = Collections.synchronizedList(new ArrayList<RouteNote>());
      List<RouteNote> preNotes = routeNotes.putIfAbsent(location, notes);
      return preNotes != null ? preNotes : notes;
    }

    private Feature checkFeature(Point location) {
      for (Feature feature : features) {
        if (feature.getLocation().getLatitude() == location.getLatitude()
            && feature.getLocation().getLongitude() == location.getLongitude()) {
          return feature;
        }
      }

      return Feature.newBuilder().setName("").setLocation(location).build();
    }

    private static double calcDistance(Point start, Point end) {
      double lat1 = RouteGuideUtil.getLatitude(start);
      double lat2 = RouteGuideUtil.getLatitude(end);
      double lon1 = RouteGuideUtil.getLongitude(start);
      double lon2 = RouteGuideUtil.getLongitude(end);
      int r = 6371000;
      double φ1 = Math.toRadians(lat1);
      double φ2 = Math.toRadians(lat2);
      double Δφ = Math.toRadians(lat2 - lat1);
      double Δλ = Math.toRadians(lon2 - lon1);

      double a = Math.sin(Δφ / 2) * Math.sin(Δφ / 2) + Math.cos(φ1) * Math.cos(φ2) * Math.sin(Δλ / 2) * Math.sin(Δλ / 2);
      double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

      return r * c;
    }
  }
}
