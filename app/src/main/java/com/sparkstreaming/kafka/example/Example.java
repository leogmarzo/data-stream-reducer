package com.sparkstreaming.kafka.example;

import java.util.Optional;

/**
 * Created by ext_lmarzo on 9/4/17.
 */
public class Example {

    public static void main(String[] args) {

       String otro = Optional.ofNullable(System.getenv("PATH")).orElse("");

        System.out.println(otro);
    }
}
