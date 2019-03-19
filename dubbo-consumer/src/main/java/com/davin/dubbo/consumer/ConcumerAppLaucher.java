package com.davin.dubbo.consumer;


import java.io.IOException;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;

@SpringBootApplication
@EnableAutoConfiguration
public class ConcumerAppLaucher {
    public static void main(String[] args) throws IOException {
        SpringApplication.run(ConcumerAppLaucher.class, args);        
    }
}
