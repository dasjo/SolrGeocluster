package geocluster;
import java.util.Map;
import java.util.Set;

import org.apache.lucene.search.Query;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.core.SolrCore;
import org.apache.solr.handler.clustering.SearchClusteringEngine;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.search.DocList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



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

/**
 * Search results clustering engine based on Carrot2 clustering algorithms.
 * <p/>
 * Output from this class is subject to change.
 *
 * @see "http://project.carrot2.org"
 */
public class GeoclusterEngine extends SearchClusteringEngine {
  private transient static Logger log = LoggerFactory
          .getLogger(GeoclusterEngine.class);

  /**
   * Name of Solr document's field containing the document's identifier. To avoid
   * repeating the content of documents in clusters on output, each cluster contains
   * identifiers of documents it contains.
   */
  private String idFieldName;

  /** Solr core we're bound to. */
  private SolrCore core;

  @Override
  @Deprecated
  public Object cluster(Query query, DocList docList, SolrQueryRequest sreq) {
    /*
    SolrIndexSearcher searcher = sreq.getSearcher();
    SolrDocumentList solrDocList;
    try {
      Map<SolrDocument,Integer> docIds = new HashMap<SolrDocument, Integer>(docList.size());
      solrDocList = SolrPluginUtils.docListToSolrDocumentList( docList, searcher, getFieldsToLoad(sreq), docIds );
      return cluster(query, solrDocList, docIds, sreq);
    } catch (IOException e) {
      throw new SolrException(ErrorCode.SERVER_ERROR, e);
    }
    */
    return null;
  }

  @Override
  public Object cluster(Query query, SolrDocumentList solrDocList,
      Map<SolrDocument, Integer> docIds, SolrQueryRequest sreq) {
    /*
    try {
      // Prepare attributes for Carrot2 clustering call
      Map<String, Object> attributes = new HashMap<String, Object>();
      List<Document> documents = getDocuments(solrDocList, docIds, query, sreq);
      attributes.put(AttributeNames.DOCUMENTS, documents);
      attributes.put(AttributeNames.QUERY, query.toString());

      // Pass the fields on which clustering runs to the
      // SolrStopwordsCarrot2LexicalDataFactory
      attributes.put("solrFieldNames", getFieldsForClustering(sreq));

      // Pass extra overriding attributes from the request, if any
      extractCarrotAttributes(sreq.getParams(), attributes);

      // Perform clustering and convert to named list
      // Carrot2 uses current thread's context class loader to get
      // certain classes (e.g. custom tokenizer/stemmer) at runtime.
      // To make sure classes from contrib JARs are available,
      // we swap the context class loader for the time of clustering.
      Thread ct = Thread.currentThread();
      ClassLoader prev = ct.getContextClassLoader();
      try {
        ct.setContextClassLoader(core.getResourceLoader().getClassLoader());
        return clustersToNamedList(controller.process(attributes,
                clusteringAlgorithmClass).getClusters(), sreq.getParams());
      } finally {
        ct.setContextClassLoader(prev);
      }
    } catch (Exception e) {
      log.error("Carrot2 clustering failed", e);
      throw new SolrException(ErrorCode.SERVER_ERROR, "Carrot2 clustering failed", e);
    }
    */
    
    
    return null;
  }

  @Override
  @SuppressWarnings({ "unchecked", "rawtypes" })
  public String init(NamedList config, final SolrCore core) {
    this.core = core;

    String result = super.init(config, core);
    final SolrParams initParams = SolrParams.toSolrParams(config);
    
    /*

    // Initialize Carrot2 controller. Pass initialization attributes, if any.
    HashMap<String, Object> initAttributes = new HashMap<String, Object>();
    extractCarrotAttributes(initParams, initAttributes);
    
    // Customize the stemmer and tokenizer factories. The implementations we provide here
    // are included in the code base of Solr, so that it's possible to refactor
    // the Lucene APIs the factories rely on if needed.
    // Additionally, we set a custom lexical resource factory for Carrot2 that
    // will use both Carrot2 default stop words as well as stop words from
    // the StopFilter defined on the field.
    final AttributeBuilder attributeBuilder = BasicPreprocessingPipelineDescriptor.attributeBuilder(initAttributes);
    attributeBuilder.lexicalDataFactory(SolrStopwordsCarrot2LexicalDataFactory.class);
    if (!initAttributes.containsKey(BasicPreprocessingPipelineDescriptor.Keys.TOKENIZER_FACTORY)) {
      attributeBuilder.tokenizerFactory(LuceneCarrot2TokenizerFactory.class);
    }
    if (!initAttributes.containsKey(BasicPreprocessingPipelineDescriptor.Keys.STEMMER_FACTORY)) {
      attributeBuilder.stemmerFactory(LuceneCarrot2StemmerFactory.class);
    }

    // Pass the schema to SolrStopwordsCarrot2LexicalDataFactory.
    initAttributes.put("solrIndexSchema", core.getSchema());

    // Customize Carrot2's resource lookup to first look for resources
    // using Solr's resource loader. If that fails, try loading from the classpath.
    DefaultLexicalDataFactoryDescriptor.attributeBuilder(initAttributes).resourceLookup(
      new ResourceLookup(
        // Solr-specific resource loading.
        new SolrResourceLocator(core, initParams),
        // Using the class loader directly because this time we want to omit the prefix
        new ClassLoaderLocator(core.getResourceLoader().getClassLoader())));

    // Carrot2 uses current thread's context class loader to get
    // certain classes (e.g. custom tokenizer/stemmer) at initialization time.
    // To make sure classes from contrib JARs are available,
    // we swap the context class loader for the time of clustering.
    Thread ct = Thread.currentThread();
    ClassLoader prev = ct.getContextClassLoader();
    try {
      ct.setContextClassLoader(core.getResourceLoader().getClassLoader());
      this.controller.init(initAttributes);
    } finally {
      ct.setContextClassLoader(prev);
    }

    SchemaField uniqueField = core.getSchema().getUniqueKeyField();
    if (uniqueField == null) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, 
          CarrotClusteringEngine.class.getSimpleName() + " requires the schema to have a uniqueKeyField");
    }
    this.idFieldName = uniqueField.getName();

    // Make sure the requested Carrot2 clustering algorithm class is available
    String carrotAlgorithmClassName = initParams.get(CarrotParams.ALGORITHM);
    Class<?> algorithmClass = core.getResourceLoader().findClass(carrotAlgorithmClassName);
    if (!IClusteringAlgorithm.class.isAssignableFrom(algorithmClass)) {
      throw new IllegalArgumentException("Class provided as "
              + CarrotParams.ALGORITHM + " must implement "
              + IClusteringAlgorithm.class.getName());
    }
    this.clusteringAlgorithmClass = (Class<? extends IClusteringAlgorithm>) algorithmClass;

    return result;
    */
    return null;
  }

  @Override
  protected Set<String> getFieldsToLoad(SolrQueryRequest sreq){
    /*
    SolrParams solrParams = sreq.getParams();

    HashSet<String> fields = Sets.newHashSet(getFieldsForClustering(sreq));
    fields.add(idFieldName);
    fields.add(solrParams.get(CarrotParams.URL_FIELD_NAME, "url"));
    fields.addAll(getCustomFieldsMap(solrParams).keySet());

    String languageField = solrParams.get(CarrotParams.LANGUAGE_FIELD_NAME);
    if (StringUtils.isNotBlank(languageField)) { 
      fields.add(languageField);
    }
    return fields;
    */
    return null;
  }

  /**
   * Returns the names of fields that will be delivering the actual
   * content for clustering. Currently, there are two such fields: document
   * title and document content.
   */
  private Set<String> getFieldsForClustering(SolrQueryRequest sreq) {
    /*
    SolrParams solrParams = sreq.getParams();

    String titleFieldSpec = solrParams.get(CarrotParams.TITLE_FIELD_NAME, "title");
    String snippetFieldSpec = solrParams.get(CarrotParams.SNIPPET_FIELD_NAME, titleFieldSpec);
    if (StringUtils.isBlank(snippetFieldSpec)) {
      throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, CarrotParams.SNIPPET_FIELD_NAME
              + " must not be blank.");
    }
    
    final Set<String> fields = Sets.newHashSet();
    fields.addAll(Arrays.asList(titleFieldSpec.split("[, ]")));
    fields.addAll(Arrays.asList(snippetFieldSpec.split("[, ]")));
    return fields;
    */
    return null;
  }
}
 
