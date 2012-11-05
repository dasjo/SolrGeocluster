package geocluster;

import ch.hsr.geohash.GeoHash;

public class GeohashHelper {
  
  public static GeoHash[] getAdjacecentNorthWest(GeoHash hash) {
    GeoHash northern = hash.getNorthernNeighbour();
    GeoHash eastern = hash.getEasternNeighbour();
    return new GeoHash[] {
        northern.getWesternNeighbour(),
        northern, 
        northern.getEasternNeighbour(),
        eastern
    };
  }

}
