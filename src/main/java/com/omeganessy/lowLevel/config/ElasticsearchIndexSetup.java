package com.omeganessy.lowLevel.config;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;

public class ElasticsearchIndexSetup {
    public static void createIndex(RestClient restClient, String index) throws IOException {
        Request request = new Request("PUT", "/" + index);
        request.setJsonEntity("{\"settings\": {\"number_of_shards\": 1, \"number_of_replicas\": 1}}");
        Response response = restClient.performRequest(request);
        System.out.println(response.getStatusLine());
    }

    public static void applyMapping(RestClient restClient, String index) throws IOException {
        String mapping = "{\n" +
                "  \"properties\": {\n" +
                "    \"title\": {\"type\": \"text\"},\n" +
                "    \"eventType\": {\"type\": \"keyword\"},\n" +
                "    \"dateTime\": {\"type\": \"text\"},\n" +
                "    \"place\": {\"type\": \"text\"},\n" +
                "    \"description\": {\"type\": \"text\"},\n" +
                "    \"subTopics\": {\"type\": \"keyword\"}\n" +
                "  }\n" +
                "}";
        Request request = new Request("PUT", "/" + index + "/_mapping");
        request.setJsonEntity(mapping);
        Response response = restClient.performRequest(request);
        System.out.println(response.getStatusLine());
    }

}
