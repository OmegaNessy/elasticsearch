package com.omeganessy;

import com.omeganessy.entity.Event;
import com.omeganessy.highLevel.service.HighLevelCommandService;
import com.omeganessy.lowLevel.service.LowLevelCommandService;
import org.elasticsearch.action.index.IndexResponse;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class Main {
    public static void main(String[] args) throws IOException {
//        LowLevelCommandService lowLevelCommandService = new LowLevelCommandService();
        HighLevelCommandService highLevelCommandService = new HighLevelCommandService();
        highLevelCommandService.storeEvent(generateEvent());
//        lowLevelCommandService.bulkDeleteEventsByTitle("Tech Talk on AI");
    }

    private static Event generateEvent(){
        Event event = new Event();
        event.setTitle("Tech Talk on AI 2.0");
        event.setEventType("tech-talk");
        event.setDateTime("2024-02-09");
        event.setPlace("Conference Room 1");
        event.setDescription("A comprehensive talk on the advancements in AI.");
        event.setSubTopics(Arrays.asList("Machine Learning", "Deep Learning", "Neural Networks"));
        return event;
    }
}