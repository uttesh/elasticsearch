/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.uttesh;

import com.uttesh.elasticsearchsample.ElasticSearchRequest;
import com.uttesh.elasticsearchsample.ElasticSearchUtil;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetRequestBuilder;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Uttesh Kumar T.H.
 */
public class ElastiSearchService {

    private static final Logger logger = LoggerFactory.getLogger(ElastiSearchService.class);

    private static ElastiSearchService instance = null;

    public static ElastiSearchService getInstance() {
        if (instance == null) {
            instance = new ElastiSearchService();
        }
        return instance;
    }

    public boolean isIndexExist(String id) {
        try {
            if (ElasticSearchUtil.getClient().admin().indices().prepareExists(id).execute().actionGet().isExists()) {
                return true;
            }
        } catch (Exception exception) {
            logger.error("isIndexExist: index error", exception);
        }

        return false;
    }

    public IndexResponse createIndex(String index, String type, String id, XContentBuilder jsonData) {
        IndexResponse response = null;
        try {
            response = ElasticSearchUtil.getClient().prepareIndex(index, type, id)
                    .setSource(jsonData)
                    .get();
            Thread.sleep(2000);
            return response;
        } catch (Exception e) {
            logger.error("createIndex", e);
        }
        return null;
    }

    public UpdateResponse updateIndex(String index, String type, String id, XContentBuilder jsonData) {
        UpdateResponse response = null;
        try {
            System.out.println("updateIndex ");
            response = ElasticSearchUtil.getClient().prepareUpdate(index, type, id)
                    .setDoc(jsonData)
                    .execute().get();
            System.out.println("response " + response);
            return response;
        } catch (Exception e) {
            logger.error("UpdateIndex", e);
        }
        return null;
    }

    public DeleteResponse removeDocument(String index, String type, String id) {
        DeleteResponse response = null;
        try {
            response = ElasticSearchUtil.getClient().prepareDelete(index, type, id).execute().actionGet();
            return response;
        } catch (Exception e) {
            logger.error("RemoveIndex", e);
        }
        return null;
    }

    public MultiGetResponse findByMultipleIndexs(List<ElasticSearchRequest> requests) {
        try {
            MultiGetRequestBuilder builder = ElasticSearchUtil.getClient().prepareMultiGet();
            for (ElasticSearchRequest _request : requests) {
                builder.add(_request.getIndex(), _request.getType(), _request.getId());
            }
            return builder.get();
        } catch (Exception e) {
            logger.error("findByMultipleIndexs", e);
        }
        return null;
    }

    public List<String> getAlldata(MultiGetResponse multiGetResponse) {
        List<String> data = new ArrayList<>();
        try {
            for (MultiGetItemResponse itemResponse : multiGetResponse) {
                GetResponse response = itemResponse.getResponse();
                if (response.isExists()) {
                    String json = response.getSourceAsString();
                    data.add(json);
                }
            }
            return data;
        } catch (Exception e) {
            logger.error("", e);
        }
        return null;
    }

    public GetResponse findDocumentByIndex(String index, String type, String id) {
        try {
            GetResponse getResponse = ElasticSearchUtil.getClient().prepareGet(index, type, id).get();
            return getResponse;
        } catch (Exception e) {
            logger.error("", e);
        }
        return null;
    }

    public SearchResponse findDocument(String index, String type, String field, String value) {
        try {
            QueryBuilder queryBuilder = new MatchQueryBuilder(field, value);
            SearchResponse response = ElasticSearchUtil.getClient().prepareSearch(index)
                    .setTypes(type)
                    .setSearchType(SearchType.QUERY_AND_FETCH)
                    .setQuery(queryBuilder)
                    .setFrom(0).setSize(60).setExplain(true)
                    .execute()
                    .actionGet();
            SearchHit[] results = response.getHits().getHits();
            return response;
        } catch (Exception e) {
            logger.error("", e);
        }
        return null;
    }
}
