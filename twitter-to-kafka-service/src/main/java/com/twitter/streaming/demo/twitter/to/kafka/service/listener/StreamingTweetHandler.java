package com.twitter.streaming.demo.twitter.to.kafka.service.listener;
import com.twitter.clientlib.api.TwitterApi;
import com.twitter.clientlib.model.*;

public abstract class StreamingTweetHandler extends StreamingHandler<StreamingTweetResponse> {
    public StreamingTweetHandler(TwitterApi apiInstance) {
        super(apiInstance);
    }

    @Override
    public StreamingTweetResponse getStreamingObject(String tweetString) throws Exception {
        return StreamingTweetResponse.fromJson(tweetString);
    }

    @Override
    public boolean hasReconnectErrors(StreamingTweetResponse streamingTweet) {
        boolean needToReconnect = false;
        if (streamingTweet.getErrors() != null) {
            for (Problem problem : streamingTweet.getErrors()) {
                if (problem instanceof OperationalDisconnectProblem || problem instanceof ConnectionExceptionProblem) {
                    System.err.println("Re-connecting to the stream due to: " + problem);
                    needToReconnect = true;
                    break;
                }
            }
        }
        return needToReconnect;
    }
}

