package com.omeganessy.highLevel.service;

import com.omeganessy.entity.Event;
import lombok.extern.log4j.Log4j2;
import org.apache.http.HttpHost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Log4j2
public class HighLevelCommandService {
    private static final String INDEX_NAME = "events";
    private static final Logger log = LogManager.getLogger(HighLevelCommandService.class);
    private final RestHighLevelClient client = new RestHighLevelClient(
            RestClient.builder(new HttpHost("localhost", 9200, "http")));


    public IndexResponse storeEvent(Event event) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        String json = objectMapper.writeValueAsString(event);

        IndexRequest indexRequest = new IndexRequest(INDEX_NAME)
                .source(json, XContentType.JSON);

        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        log.info("Document indexed with id: {}", indexResponse.getId());
        System.out.println("Document indexed with id: " + indexResponse.getId());
        return indexResponse;
    }

    public List<Event> retrieveAll() throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Event> events = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Event event = objectMapper.readValue(hit.getSourceAsString(), Event.class);
            event.setId(hit.getId());
            events.add(event);
        }
        return events;
    }

    public List<Event> retrieveWorkshopEvents() throws IOException {
        // Inside the main method
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.termQuery("eventType", "workshop"));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Event> events = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Event event = objectMapper.readValue(hit.getSourceAsString(), Event.class);
            event.setId(hit.getId());
            events.add(event);
        }
        return events;
    }

    public List<Event> retrieveEventsByTitle(String specificTitle) throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchQuery("title", specificTitle));
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Event> events = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Event event = objectMapper.readValue(hit.getSourceAsString(), Event.class);
            event.setId(hit.getId());
            events.add(event);
        }
        return events;
    }

    public List<Event> retrieveEventsByDateAndTitle(String date, String specificTitle) throws IOException {
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                .must(QueryBuilders.matchQuery("title", specificTitle))
                .must(QueryBuilders.rangeQuery("dateTime").gt(date));

        searchSourceBuilder.query(boolQuery);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        List<Event> events = new ArrayList<>();
        ObjectMapper objectMapper = new ObjectMapper();

        for (SearchHit hit : searchResponse.getHits().getHits()) {
            Event event = objectMapper.readValue(hit.getSourceAsString(), Event.class);
            event.setId(hit.getId());
            events.add(event);
        }
        return events;
    }

    public void bulkDeleteEventsByTitle(String title) throws IOException {
        BulkRequest request = new BulkRequest();
        retrieveEventsByTitle(title).stream().forEach(event -> request.add(new DeleteRequest(INDEX_NAME, event.getId())));
        client.bulk(request, RequestOptions.DEFAULT);
    }

}
