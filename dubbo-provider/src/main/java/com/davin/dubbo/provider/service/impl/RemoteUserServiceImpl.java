package com.davin.dubbo.provider.service.impl;

import org.apache.dubbo.config.annotation.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import com.davin.dubbo.common.service.RemoteUserService;
 


@Service
public class RemoteUserServiceImpl implements RemoteUserService {
    @Override
    public String sayHello(String name) {
        return "Hello "+name;
    }
    
}
