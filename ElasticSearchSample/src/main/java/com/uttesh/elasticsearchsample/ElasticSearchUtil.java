/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.uttesh.elasticsearchsample;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Uttesh Kumar T.H.
 */
public class ElasticSearchUtil {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchUtil.class);
    private static final String CLUSTER_NAME = "sampledata-elasticsearch";
    private static final String ELASTIC_SERVER_HOST = "localhost";
    private static final int ELASTIC_SERVER_PORT = 9200;
    private static final int ELASTIC_INDEX_PORT = 9300;

    public static TransportClient getTransportClient(String cluster, String host, int port) throws UnknownHostException {
        try {
            cluster = cluster == null ? CLUSTER_NAME : cluster;
            host = host == null ? ELASTIC_SERVER_HOST : host;
            port = port == 0 ? ELASTIC_SERVER_PORT : port;

            Settings settings = Settings.settingsBuilder()
                    .put(ElasticConstants.CLUSTER_NAME, cluster).build();
            TransportClient transportClient = TransportClient.builder().settings(settings).build().
                    addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port));
            return transportClient;
        } catch (Exception e) {
            logger.error("getTransportClient", e);
        }
        return null;
    }

    public static Client getClient() {
        try {
            return getTransportClient(CLUSTER_NAME, ELASTIC_SERVER_HOST, ELASTIC_INDEX_PORT);
        } catch (Exception e) {
            logger.error("getClient", e);
        }
        return null;
    }

    public List<SearchResponse> getAllMultiResponseHits(MultiSearchResponse MultiSearchResponse) {
        try {
            List<SearchResponse> result = new ArrayList<SearchResponse>();
            for (MultiSearchResponse.Item item : MultiSearchResponse.getResponses()) {
                SearchResponse response = item.getResponse();
                result.add(response);
            }
            return result;
        } catch (Exception e) {
            logger.error("", e);
        }
        return null;
    }
}
