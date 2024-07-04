package com.omeganessy.highLevel.config;

import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.*;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.PutMappingRequest;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;

public class ElasticsearchIndexSetup {
    public static void createIndex(RestHighLevelClient restClient, String index) throws IOException {
        CreateIndexRequest request = new CreateIndexRequest(index);
        CreateIndexResponse createIndexResponse = restClient.indices().create(request, RequestOptions.DEFAULT);
        System.out.println("Index created: " + createIndexResponse.index());
        PutMappingRequest mappingRequest = putMappingReq(index);
        AcknowledgedResponse response = restClient.indices().putMapping(mappingRequest, RequestOptions.DEFAULT);
        System.out.println("Mapping applied: " + response.isAcknowledged());
    }


    private static PutMappingRequest putMappingReq(String index) throws IOException {
        PutMappingRequest mappingRequest = new PutMappingRequest(index);
        try {
            mappingRequest.source(XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("title")
                    .field("type", "text")
                    .endObject()
                    .startObject("eventType")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("dateTime")
                    .field("type", "text")
                    .endObject()
                    .startObject("place")
                    .field("type", "text")
                    .endObject()
                    .startObject("description")
                    .field("type", "text")
                    .endObject()
                    .startObject("subTopics")
                    .field("type", "text")
                    .endObject()
                    .endObject()
                    .endObject());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return mappingRequest;

    }

}
