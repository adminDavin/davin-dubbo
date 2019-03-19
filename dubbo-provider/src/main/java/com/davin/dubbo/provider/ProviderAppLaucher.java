package com.davin.dubbo.provider;


import java.io.IOException;

 
import org.springframework.boot.autoconfigure.EnableAutoConfiguration; 
import org.springframework.boot.builder.SpringApplicationBuilder;
 
 
 
@EnableAutoConfiguration
public class ProviderAppLaucher {
    public static void main(String[] args) throws IOException {
        new SpringApplicationBuilder(ProviderAppLaucher.class).run(args);
    }
}
