package com.epam.learning.messageorientedmiddleware.kafkastreams.bean;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class Person {

    private String name;
    private String company;
    private String position;
    private int experience;

}
