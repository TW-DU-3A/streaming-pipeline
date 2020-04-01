package com.tw;

import com.tw.services.ApiProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class ProducerScheduler {

    private static Logger logger = LoggerFactory.getLogger(ProducerScheduler.class);

    @Autowired
    private ApiProducer apiProducer;

    @Value("${producer.url}")
    private String url;

    @Scheduled(cron = "${producer.cron}")
    public void scheduledProducer() {

        RestTemplate template = new RestTemplate();
        HttpEntity<String> response = template.exchange(url, HttpMethod.GET, HttpEntity.EMPTY, String.class);
        logger.info("URL: " + url);
        apiProducer.sendMessage(response);
    }
}
