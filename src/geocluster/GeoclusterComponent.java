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
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

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
 */
public class GeoclusterComponent extends SearchComponent implements SolrCoreAware {
  private transient static Logger log = LoggerFactory.getLogger(GeoclusterComponent.class);

  /**
   * Base name for all spell checker query parameters. This name is also used to
   * register this component with SearchHandler.
   */
  public static final String COMPONENT_NAME = "geocluster";
  private NamedList initParams;
  
  // TODO: make this a parameter
  protected String groupField = "f_ss_field_place:geohash_geocluster_index_3";
  
  @Override
  public void prepare(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (params.getBool(COMPONENT_NAME, false)) {
      
      // We rely on a docList to access data to cluster upon.
      // TODO: this just doesn't work
      // rb.setNeedDocList( true );
      // Alternative workaround, see getDocList in Grouping.java
      rb.setFieldFlags(SolrIndexSearcher.GET_DOCLIST);
    }
  }

  @Override
  public void process(ResponseBuilder rb) throws IOException {
    SolrParams params = rb.req.getParams();
    if (!params.getBool(COMPONENT_NAME, false)) {
      return;
    }
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
      // Temp. store for clusters.
      int size = results.docList.size(); // will be less, but no way to calc in advance?
      Map<String, SolrDocument> clusterMap = new HashMap<String, SolrDocument>(size);
      
      for (NamedList group : groups) {
        String geohashPrefix = (String)group.get("groupValue");
        log.info("Prefix: " + geohashPrefix);
        
        // Add all points within the core geohash to a cluster.
        
        SolrDocument cluster = null;

        DocSlice docList = (DocSlice)group.get("doclist");
        DocIterator iterator = docList.iterator();
        while (iterator.hasNext()) {
          Integer docId = iterator.next();
          SolrDocument doc = docIds.get(docId);
          
          String geohash = (String)doc.getFieldValue("ss_field_place:geohash");
          String latlon = (String)doc.getFieldValue("t_field_place:latlon");
          String id = (String)doc.getFieldValue("ss_search_api_id");
          
          // Init cluster
          if (cluster == null) {
            cluster = initCluster(doc, docId, clusterMap, geohashPrefix, docList);
          }
          else {
            addCluster(cluster, doc, docId);
          }
          log.info("Doc: " + id + ", geohash: " + geohash + ", latlon: " + latlon); 
        }
        
        GeoHash hash = GeoHash.fromGeohashString(geohashPrefix);
        GeoHash[] neighbors = hash.getAdjacent();
        for (GeoHash neighbor : neighbors) {
          String neighborHashString = neighbor.toString();
          if (clusterMap.containsKey(neighborHashString)) {
            SolrDocument otherCluster = clusterMap.get(neighborHashString);
            if (shouldCluster(cluster, otherCluster)) {
              mergeCluster(cluster, otherCluster);
            }
          }
        }
      }
      
      foreach(NamedList test : clusterMap.entrySet()) {
        
      }
    }
    
    for (NamedList namedList : clusterMap.) {
      
    }
    
    // Object clusters = engine.cluster(rb.getQuery(), solrDocList, docIds, rb.req);
    // rb.rsp.add("clusters", clusters);
  }

  private void mergeCluster(SolrDocument cluster, SolrDocument otherCluster) {
    HashMap<Integer, SolrDocument> docs = (HashMap<Integer, SolrDocument>) otherCluster.getFieldValue("docs");
    for (Entry<Integer, SolrDocument> entry : docs.entrySet()) {
      addCluster(cluster, entry.getValue(), entry.getKey());
    }
  }

  private boolean shouldCluster(SolrDocument cluster, SolrDocument otherCluster) {
    // TODO Auto-generated method stub
    return true;
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

  /**
   * Initialize a cluster.
   * 
   * @param clusterMap
   * @param geohashPrefix
   * @param docList
   * @return 
   */
  private SolrDocument initCluster(SolrDocument doc, Integer docId, Map<String, SolrDocument> clusterMap, String geohashPrefix, DocSlice docList) {
    HashMap<Integer, SolrDocument> docs = new HashMap<Integer, SolrDocument>();
    docs.put(docId, doc);
    SolrDocument cluster = new SolrDocument();
    clusterMap.put(geohashPrefix, cluster);
    cluster.addField("docs", docs);
    cluster.addField("count", docList.size());
    return cluster;
  }
  
  /**
   * Returns the set of field names to load.
   * Concrete classes can override this method if needed.
   * Default implementation returns null, that is, all stored fields are loaded.
   * @param sreq
   * @return set of field names to load
   */
  protected Set<String> getFieldsToLoad(SolrQueryRequest sreq){
    return null;
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
