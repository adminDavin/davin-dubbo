package com.davin.dubbo.consumer.controller;

import com.davin.dubbo.common.service.RemoteUserService;
import org.apache.dubbo.config.annotation.Reference;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
 

@RestController
public class UserController {
    @Reference
    private RemoteUserService remoteUserService;
    
    @RequestMapping("/sayHello")
    public String index() {
        return remoteUserService.sayHello("ddddddd");
    }
}
