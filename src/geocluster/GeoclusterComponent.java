package geocluster;
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.lucene.spatial.geometry.DistanceUnits;
import org.apache.lucene.spatial.geometry.FloatLatLng;
import org.apache.lucene.spatial.geometry.LatLng;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.clustering.ClusteringParams;
import org.apache.solr.handler.component.ResponseBuilder;
import org.apache.solr.handler.component.SearchComponent;
import org.apache.solr.handler.component.ShardRequest;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocIterator;
import org.apache.solr.search.DocList;
import org.apache.solr.search.DocListAndSet;
import org.apache.solr.search.DocSlice;
import org.apache.solr.search.SolrIndexSearcher;
import org.apache.solr.util.SolrPluginUtils;
import org.apache.solr.util.plugin.SolrCoreAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.hsr.geohash.GeoHash;


/**
 * Provide a plugin for geo-clustering results. 
 * http://localhost:8123/solr/search?facet=true&group.limit=5&geocluster.clusterField=field_place&geocluster.resolution=1221.96875&geocluster.zoomLevel=7&fl=*&geocluster.idField=ss_search_api_id&group.field=f_ss_field_place:geohash_geocluster_index_3&facet.field=f_ss_field_place:geohash_geocluster_index_3&qt=/geocluster&fq=index_id:geocluster_index&geocluster.clusterDistance=33&geocluster.groupField=f_ss_field_place:geohash_geocluster_index_3&geocluster.geohashField=geohashs_field_place:geohash&qf=t_field_place:latlon^1.0&geocluster.geohashLength=3&rows=1000000&start=0&q=*:*&facet.prefix=3_&geocluster.latlonField=t_field_place:latlon&group=true
 */
public class GeoclusterComponent extends SearchComponent implements SolrCoreAware {
  private transient static Logger log = LoggerFactory.getLogger(GeoclusterComponent.class);

  /**
   * Base name for all spell checker query parameters. This name is also used to
   * register this component with SearchHandler.
   */
  public static final String COMPONENT_NAME = "geocluster";
  private NamedList initParams;
  
  protected boolean initialized = false;
  
  /**
   * Cluster distance in pixels.
   */
  protected double clusterDistance;
  
  /**
   * Current zoom level for clustering.
   */
  protected int zoomLevel;
  
  /**
   * Resolution in meters / pixel based on zoom_level.
   */
  protected double resolution;
  
  /**
   * Geohash length for clustering by a specified distance in pixels.
   */
  protected int geohashLength;
  
  /**
   * Field used for clustering, for example field_place.
   */
  protected String clusterField;
  
  /**
   * Field used for grouping, for example f_ss_field_place:geohash_geocluster_index_3.
   */
  protected String groupField;
  
  protected String geohashField;
  
  protected String idField;
  
  protected String latlonField;
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (params.getBool(COMPONENT_NAME, false)) {
      // We rely on a docList to access data to cluster upon.
      // TODO: this just doesn't work
      // rb.setNeedDocList( true );
      // Alternative workaround, see getDocList in Grouping.java
      rb.setFieldFlags(SolrIndexSearcher.GET_DOCLIST);
      
      this.initialized = false;
      try {
        // Parse geocluster specific query parameters.
        // These are provided by GeoclusterSearchApiSolrService.
        this.clusterDistance = Double.parseDouble(params.get("geocluster.clusterDistance"));
        this.zoomLevel = Integer.parseInt(params.get("geocluster.zoomLevel"));
        this.resolution = Double.parseDouble(params.get("geocluster.resolution"));
        this.geohashLength = Integer.parseInt(params.get("geocluster.geohashLength"));
        this.clusterField = params.get("geocluster.clusterField");
        this.groupField = params.get("geocluster.groupField");
        this.geohashField = params.get("geocluster.geohashField");
        this.idField = params.get("geocluster.idField");
        this.latlonField = params.get("geocluster.latlonField");
        this.initialized = true;
      }
      catch (Exception e) {
      }
    }
    
    this.clusterDistance = 200;
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false) || !this.initialized) {
      log.info("skipped clustering");
      return;
    }
    log.info("started clustering");
    DocListAndSet results = rb.getResults();
    Map<SolrDocument,Integer> docIdsReverse = new HashMap<SolrDocument, Integer>(results.docList.size());
    SolrDocumentList solrDocList = getSolrDocumentList(results.docList, rb.req, docIdsReverse);
    
    Map<Integer, SolrDocument> docIds = new HashMap<Integer, SolrDocument>(results.docList.size());
    
    for (Entry<SolrDocument, Integer> docId : docIdsReverse.entrySet()) {
      docIds.put(docId.getValue(), docId.getKey());
    }
    
    // Get grouped values.
    NamedList values = rb.rsp.getValues();
    NamedList grouped = (NamedList)values.get("grouped");
    NamedList groupedValue = (NamedList)grouped.get(groupField);
    ArrayList<NamedList> groups = (ArrayList)groupedValue.get("groups");
    
    // Iterate over grouped values and perform clustering algorithm.
    if (groups != null) {
      
      // Add all points within the core geohash to a cluster.
      Map<String, SolrDocument> clusterMap = clusterByHashes(docIds, groups);
      
      // Compare neighbor overlaps.
      neighborCheck(clusterMap);
      
      // Finalize cluster data.
      finalizeClusters(clusterMap);
      
      // Remove normal grouped docs from response.
      rb.rsp.getValues().remove(rb.rsp.getValues().indexOf("grouped", 0));
      
      // Add our custom cluster map instead.
      rb.rsp.add("clusters", clusterMap);
    }
        
    log.info("clustering finished");
  }

  private Map<String, SolrDocument> clusterByHashes(Map<Integer, SolrDocument> docIds, ArrayList<NamedList> groups) {
    int size = docIds.size(); // will be less, but no way to calc in advance?
    Map<String, SolrDocument> clusterMap = new HashMap<String, SolrDocument>(size);
  
    // Add all points within the core geohash to a cluster.
    for (NamedList group : groups) {
      String geohashPrefix = (String)group.get("groupValue");
      log.info("Prefix: " + geohashPrefix);
      
      SolrDocument cluster = null;
      DocSlice docList = (DocSlice)group.get("doclist");
      DocIterator iterator = docList.iterator();
      while (iterator.hasNext()) {
        Integer docId = iterator.next();
        SolrDocument doc = docIds.get(docId);
        
        String geohash = (String)doc.getFieldValue(this.geohashField);
        String latlon = (String)doc.getFieldValue(this.latlonField);
        String id = (String)doc.getFieldValue(this.idField);
        
        // Init cluster
        if (cluster == null) {
          cluster = initCluster(doc, docId, clusterMap, geohashPrefix, docList);
          log.info("Parent: " + id + ", geohash: " + geohash + ", latlon: " + latlon); 
        }
        else {
          addCluster(cluster, doc, docId);
          log.info("Child : " + id + ", geohash: " + geohash + ", latlon: " + latlon); 
        }
      }
      
      updateCluster(cluster);
    }
    return clusterMap;
  }

  private void neighborCheck(Map<String, SolrDocument> clusterMap) {
    Iterator<Entry<String, SolrDocument>> i = clusterMap.entrySet().iterator();
    /* 
     * TODO: we are always merging the current cluster into the other one
     * if those overlap. this is for coding convenience as we can use the
     * iterator.remove method this way.
     * 
     * actually we should check where in which geohash quadrant the new
     * super cluster will remain and choose which to delete upon that.
     */
    loop:
    while (i.hasNext()) {
      Entry<String, SolrDocument> clusterEntry = i.next();
      String geohashPrefix = clusterEntry.getKey();
      if (geohashPrefix == null) {
        continue;
      }
      SolrDocument cluster = clusterEntry.getValue();
      log.info("Cluster: key: " + geohashPrefix + ", value: " );
      GeoHash hash = GeoHash.fromGeohashString(geohashPrefix);
      
      // Get all neighbors to check for.
      GeoHash[] neighbors = GeohashHelper.getAdjacecentNorthWest(hash);
      for (GeoHash neighbor : neighbors) {
        String neighborHashString = neighbor.toBase32();
        if (clusterMap.containsKey(neighborHashString)) {
          SolrDocument otherCluster = clusterMap.get(neighborHashString);
          
          // For every neighbor we check if they overlap and if so we merge.
          if (shouldCluster(otherCluster, cluster)) {
            mergeCluster(otherCluster, cluster);
            i.remove();
            continue loop;  // This cluster is gone, remove and continue.
          }
        }
      }
    }
  }

  private void finalizeClusters(Map<String, SolrDocument> clusterMap) {
    for (Entry<String, SolrDocument> clusterEntry : clusterMap.entrySet()) {
      String geohashPrefix = clusterEntry.getKey();
      if (geohashPrefix == null) {
        continue;
      }
      SolrDocument cluster = clusterEntry.getValue();
      this.finishCluster(cluster);
    }
  }

  /**
   * Initialize a cluster.
   */
  private SolrDocument initCluster(SolrDocument doc, Integer docId, Map<String, SolrDocument> clusterMap, String geohashPrefix, DocSlice docList) {
    HashMap<Integer, SolrDocument> docs = new HashMap<Integer, SolrDocument>();
    docs.put(docId, doc);
    SolrDocument cluster = new SolrDocument();
    clusterMap.put(geohashPrefix, cluster);
    cluster.addField("docs", docs);
    return cluster;
  }

  /**
   * Add a document to a cluster.
   * 
   * @param clusterMap
   * @param doc
   * @param clusterMap
   */
  private void addCluster(SolrDocument cluster, SolrDocument doc, Integer docId) {
    HashMap<Integer, SolrDocument> docs = (HashMap<Integer, SolrDocument>)cluster.getFieldValue("docs");
    docs.put(docId, doc);
  }

  private void updateCluster(SolrDocument cluster) {
    // Calculate center point from all clustered points.
    HashMap<Integer, SolrDocument> docs = (HashMap<Integer, SolrDocument>)cluster.getFieldValue("docs");
    Float latMin = null, latMax = null, lonMin = null, lonMax = null;
    for (Entry<Integer, SolrDocument> entry : docs.entrySet()) {
      SolrDocument doc = entry.getValue();
      String latlon = (String)doc.getFieldValue(this.latlonField);
      if (latlon != null) {
        String[] latlonSplit = latlon.split(",");
        float lat = Float.parseFloat(latlonSplit[0]);
        float lon = Float.parseFloat(latlonSplit[1]);
        latMin = latMin == null ? lat : Math.min(latMin, lat);
        latMax = latMax == null ? lat : Math.max(latMax, lat);
        lonMin = lonMin == null ? lon : Math.min(lonMin, lon);
        lonMax = lonMax == null ? lon : Math.max(lonMax, lon);
      }
    }
    try {
      LatLng latlonCenter = new FloatLatLng(
          (latMin + latMax) / 2, 
          (lonMin + lonMax) / 2
      );
      cluster.put("center", latlonCenter);
    }
    catch(Exception e) {
    }
  }

  private boolean shouldCluster(SolrDocument cluster, SolrDocument otherCluster) {
    LatLng latlng = (LatLng)cluster.get("center");
    LatLng latlngOther = (LatLng)otherCluster.get("center");
    // Calculate distance in meters.
    double distance = latlng.arcDistance(latlngOther, DistanceUnits.KILOMETERS);
    log.info("distance: " + distance);
    return distance < this.clusterDistance;
  }

  private void mergeCluster(SolrDocument cluster, SolrDocument otherCluster) {
    HashMap<Integer, SolrDocument> otherDocs = (HashMap<Integer, SolrDocument>) otherCluster.getFieldValue("docs");
    for (Entry<Integer, SolrDocument> otherEntry : otherDocs.entrySet()) {
      addCluster(cluster, otherEntry.getValue(), otherEntry.getKey());
    }
    // Uddate center.
    LatLng center = (LatLng)cluster.get("center");
    LatLng otherCenter = (LatLng)otherCluster.get("center");
    LatLng newCenter = center.calculateMidpoint(otherCenter);
    cluster.setField("center", newCenter);
  }

  private void finishCluster(SolrDocument cluster) {
    // Replace center with latlng string.
    LatLng center = (LatLng)cluster.get("center");
    cluster.setField("center", center.getLat() + "," + center.getLng());
    // Replace docs with ids only.
    HashMap<Integer, SolrDocument> docs = (HashMap<Integer, SolrDocument>)cluster.getFieldValue("docs");
    cluster.setField("docs", docs.keySet());
    cluster.addField("count", docs.size());
  }

  /**
   * Returns the set of field names to load.
   * Concrete classes can override this method if needed.
   * Default implementation returns null, that is, all stored fields are loaded.
   * @param sreq
   * @return set of field names to load
   */
  protected Set<String> getFieldsToLoad(SolrQueryRequest sreq) {
    // TODO: still complete documents seem to be loaded.
    Set<String> fields = new HashSet<String>();
    fields.add(this.groupField);
    fields.add(this.geohashField);
    fields.add(this.idField);
    fields.add(this.latlonField);
    return fields;
  }

  protected SolrDocumentList getSolrDocumentList(DocList docList, SolrQueryRequest sreq,
      Map<SolrDocument, Integer> docIds) throws IOException{
    return SolrPluginUtils.docListToSolrDocumentList(
        docList, sreq.getSearcher(), getFieldsToLoad(sreq), docIds);
  }

  @Override
  public void modifyRequest(ResponseBuilder rb, SearchComponent who, ShardRequest sreq) {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false) || !params.getBool(ClusteringParams.USE_SEARCH_RESULTS, false)) {
      return;
    }
    sreq.params.remove(COMPONENT_NAME);
  }

  @Override
  @SuppressWarnings("unchecked")
  public void init(NamedList args) {
    super.init(args);
    this.initParams = args;
  }

  public void inform(SolrCore core) {
  }

  // ///////////////////////////////////////////
  // / SolrInfoMBean
  // //////////////////////////////////////////

  @Override
  public String getDescription() {
    return "A Clustering component";
  }

  @Override
  public String getVersion() {
    return "$Revision$";
  }

  @Override
  public String getSourceId() {
    return "$Id$";
  }

  @Override
  public String getSource() {
    return "$URL$";
  }

}
