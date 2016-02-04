package io.grpc.examples.routeguide;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonValue;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Tao Li on 2016/2/4.
 */
public class RouteGuideUtil {
  private static final double COORD_FACTOR = 1e7;

  public static double getLatitude(Point location) {
    return location.getLatitude() / COORD_FACTOR;
  }

  public static double getLongitude(Point location) {
    return location.getLongitude() / COORD_FACTOR;
  }

  public static URL getDefaultFeaturesFile() {
    return RouteGuideServer.class.getClassLoader().getResource("route_guide_db.json");
  }

  public static List<Feature> parseFeatures(URL file) throws IOException {
    InputStream input = file.openStream();
    try {
      JsonReader reader = Json.createReader(input);
      List<Feature> features = new ArrayList<>();
      for (JsonValue value : reader.readArray()) {
        JsonObject obj = (JsonObject) value;
        String name = obj.getString("name", "");
        JsonObject location = obj.getJsonObject("location");
        int lat = location.getInt("latitude");
        int lon = location.getInt("longitude");
        Feature feature = Feature.newBuilder().setName(name).setLocation(
            Point.newBuilder().setLatitude(lat).setLongitude(lon).build()
        ).build();
        features.add(feature);
      }
      return features;
    } finally {
      input.close();
    }
  }

  public static boolean exists(Feature feature) {
    return feature != null && !feature.getName().isEmpty();
  }
}
