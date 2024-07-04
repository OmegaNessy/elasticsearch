package com.omeganessy.lowLevel.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.omeganessy.entity.Event;
import com.omeganessy.lowLevel.config.ElasticsearchIndexSetup;
import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class LowLevelCommandService {
    private static final String INDEX_NAME = "events";
    private final RestClient client;
    private final ObjectMapper objectMapper = new ObjectMapper();


    public LowLevelCommandService() {
        client = RestClient.builder(
                new HttpHost("localhost", 9200, "http")).build();

    }
    public void setupIndex() throws IOException {
        ElasticsearchIndexSetup.createIndex(client, INDEX_NAME);
        ElasticsearchIndexSetup.applyMapping(client, INDEX_NAME);
    }

    public void storeEvent(Event event) throws IOException {
        Request request = new Request("POST", "/" + INDEX_NAME + "/_doc");
        String jsonString = objectMapper.writeValueAsString(event);
        request.setJsonEntity(jsonString);
        Response response = client.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        System.out.println(responseBody);
    }

    public List<Event> searchAllEvents() throws IOException {
        Request request = new Request("GET", "/" + INDEX_NAME + "/_search");
        Response response = client.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonNode hits = objectMapper.readTree(responseBody).get("hits").get("hits");
        List<Event> events = new ArrayList<>();

        if (hits.isArray()) {
            for (JsonNode hit : hits) {
                JsonNode source = hit.get("_source");
                Event event = objectMapper.treeToValue(source, Event.class);
                event.setId(hit.get("_id").asText());
                events.add(event);
            }
        }
        return events;
    }

    public List<Event> searchWorkshopEvents() throws IOException {
        Request request = new Request("GET", "/" + INDEX_NAME + "/_search");
        String query = """
            {
                "query": {
                    "term": {
                        "eventType": "workshop"
                    }
                }
            }
        """;
        request.setJsonEntity(query);
        Response response = client.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonNode hits = objectMapper.readTree(responseBody).get("hits").get("hits");
        List<Event> events = new ArrayList<>();

        if (hits.isArray()) {
            for (JsonNode hit : hits) {
                JsonNode source = hit.get("_source");
                Event event = objectMapper.treeToValue(source, Event.class);
                event.setId(hit.get("_id").asText());
                events.add(event);
            }
        }
        return events;
    }

    public List<Event> searchEventsByTitle(String title) throws IOException {
        Request request = new Request("GET", "/" + INDEX_NAME + "/_search");
        String query = String.format("""
            {
                "query": {
                    "match": {
                        "title": "%s"
                    }
                }
            }
        """, title);
        request.setJsonEntity(query);
        Response response = client.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonNode hits = objectMapper.readTree(responseBody).get("hits").get("hits");
        List<Event> events = new ArrayList<>();

        if (hits.isArray()) {
            for (JsonNode hit : hits) {
                JsonNode source = hit.get("_source");
                Event event = objectMapper.treeToValue(source, Event.class);
                event.setId(hit.get("_id").asText());
                events.add(event);
            }
        }
        return events;
    }

    public List<Event> searchEventsAfterDateWithName(String date, String title) throws IOException {
        Request request = new Request("GET", "/" + INDEX_NAME + "/_search");
        String query = String.format("""
            {
                "query": {
                    "bool": {
                        "must": [
                            { "match": { "title": "%s" }},
                            { "range": { "dateTime": { "gt": "%s" }}}
                        ]
                    }
                }
            }
        """, title, date);
        request.setJsonEntity(query);
        Response response = client.performRequest(request);
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonNode hits = objectMapper.readTree(responseBody).get("hits").get("hits");
        List<Event> events = new ArrayList<>();

        if (hits.isArray()) {
            for (JsonNode hit : hits) {
                JsonNode source = hit.get("_source");
                Event event = objectMapper.treeToValue(source, Event.class);
                event.setId(hit.get("_id").asText());
                events.add(event);
            }
        }
        return events;
    }

    public void bulkDeleteEventsByTitle(String title) throws IOException {
        StringBuilder bulkRequestBody = new StringBuilder();
        searchEventsByTitle(title).stream().forEach(event -> {
            String deleteAction = """
                { "delete": { "_index": "%s", "_id": "%s" } }
            """.formatted(INDEX_NAME, event.getId());
            bulkRequestBody.append(deleteAction).append("\n");
        });
        Request request = new Request("POST", "/_bulk");
        request.setJsonEntity(bulkRequestBody.toString());
        client.performRequest(request);
    }
}
