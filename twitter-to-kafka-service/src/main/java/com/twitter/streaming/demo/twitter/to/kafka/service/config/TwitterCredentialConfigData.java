package com.twitter.streaming.demo.twitter.to.kafka.service.config;

import lombok.Data;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

import java.util.List;

@Data
@Configuration
@PropertySource("classpath:twitter4j.properties")
public class TwitterCredentialConfigData {
    @Value("${bearer.token}")
    private String BEARER_TOKEN;
}
