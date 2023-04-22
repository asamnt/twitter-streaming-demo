package com.twitter.streaming.demo.twitter.to.kafka.service.runner.impl;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.TwitterCredentialsBearer;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.streaming.demo.twitter.to.kafka.service.config.TwitterCredentialConfigData;
import com.twitter.streaming.demo.twitter.to.kafka.service.config.TwitterToKafkaServiceConfigData;
import com.twitter.streaming.demo.twitter.to.kafka.service.listener.StreamingTweetHandlerImpl;
import com.twitter.streaming.demo.twitter.to.kafka.service.listener.TweetsStreamListenersExecutor;
import com.twitter.streaming.demo.twitter.to.kafka.service.listener.TwitterKafkaStatusListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.stereotype.Component;
import twitter4j.FilterQuery;
import twitter4j.TwitterException;
import twitter4j.TwitterStream;
import com.twitter.clientlib.model.*;
import twitter4j.TwitterStreamFactory;

import javax.annotation.PreDestroy;
import java.util.Arrays;

@Component
@ConditionalOnExpression("${twitter-to-kafka-service.enable-mock-tweets} && not ${twitter-to-kafka-service.enable-v2-tweets}")
public class TwitterKafkaStreamRunner {

    private static final Logger logger = LoggerFactory.getLogger(TwitterKafkaStreamRunner.class);

    private final TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData;

    private final TwitterCredentialConfigData twitterCredentialConfigData;

    private final TwitterKafkaStatusListener twitterKafkaStatusListener;

    private TwitterStream twitterStream;

    public TwitterKafkaStreamRunner(TwitterToKafkaServiceConfigData twitterToKafkaServiceConfigData,
                                    TwitterCredentialConfigData twitterCredentialConfigData, TwitterKafkaStatusListener twitterKafkaStatusListener) {
        this.twitterToKafkaServiceConfigData = twitterToKafkaServiceConfigData;
        this.twitterCredentialConfigData = twitterCredentialConfigData;
        this.twitterKafkaStatusListener = twitterKafkaStatusListener;
    }


    public void start() throws TwitterException, ApiException {
        TwitterApi apiInstance = new TwitterApi(new TwitterCredentialsBearer(twitterCredentialConfigData.getBEARER_TOKEN()));

//        twitterStream= new TwitterStreamFactory().getInstance();
//        twitterStream.addListener(twitterKafkaStatusListener);
//        addFilter();
        try {
            TweetsStreamListenersExecutor tsle = new TweetsStreamListenersExecutor();
            tsle.stream()
                    .streamingHandler(new StreamingTweetHandlerImpl(apiInstance))
                    .executeListeners();
            while (tsle.getError() == null) {
                try {
                    System.out.println("==> sleeping 5 ");
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            if (tsle.getError() != null) {
                System.err.println("==> Ended with error: " + tsle.getError());
            }
        } catch (ApiException e) {
            System.err.println("Status code: " + e.getCode());
            System.err.println("Reason: " + e.getResponseBody());
            System.err.println("Response headers: " + e.getResponseHeaders());
            e.printStackTrace();
        }

    }

    @PreDestroy
    public void shutdown() {
        if (twitterStream != null) {
            logger.info("Closing twitter stream");
            twitterStream.shutdown();
        }

    }

    private void addFilter() {
        String[] keywords = twitterToKafkaServiceConfigData.getTwitterKeywords().toArray(new String[0]);
        FilterQuery filterQuery = new FilterQuery(keywords);
        twitterStream.filter(filterQuery);
        logger.info("Started filtering twitter stream for keywords {}", Arrays.toString(keywords));
    }
}
