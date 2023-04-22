package com.twitter.streaming.demo.twitter.to.kafka.service.listener;
import java.io.InputStream;

import com.twitter.clientlib.ApiException;
import com.twitter.clientlib.api.TwitterApi;

public abstract class StreamingHandler<T> {
    protected final TwitterApi apiInstance;

    public StreamingHandler(TwitterApi apiInstance) {
        this.apiInstance = apiInstance;
    }

    public abstract InputStream connectStream() throws ApiException;
    public abstract void actionOnStreamingObject(T streamingTweet) throws ApiException;
    public abstract T getStreamingObject(String tweetString) throws Exception;
    public abstract boolean hasReconnectErrors(T streamingTweet);

    public boolean processAndVerifyStreamingObject(String tweetString) throws Exception {
        T tweet = getStreamingObject(tweetString);
        actionOnStreamingObject(tweet);
        return !hasReconnectErrors(tweet);
    }
}
