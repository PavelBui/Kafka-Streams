package com.epam.learning.messageorientedmiddleware.kafkastreams.bean;

import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.boot.jackson.JsonComponent;

@JsonComponent
@NoArgsConstructor
@Data
@Builder
public class Person {

    private String name;
    private String company;
    private String position;
    private int experience;

    public Person(String name, String company, String position, int experience) {
        this.name = name;
        this.company = company;
        this.position = position;
        this.experience = experience;
    }


}
