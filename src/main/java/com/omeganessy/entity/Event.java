package com.omeganessy.entity;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;
import java.util.List;

@Getter
@Setter
@ToString
public class Event {
    private String id;
    private String title;
    private String eventType;
    private String dateTime;
    private String place;
    private String description;
    private List<String> subTopics;
}
