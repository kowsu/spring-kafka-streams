package com.rk.event.model;

/*
    @created August/21/2023 - 11:17 PM
    @project kafka-streams-example
    @author k.ramanjineyulu
*/

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Person {

    private String name, email;
    private double salary;
    private String dept;
}
