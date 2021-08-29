package com.jack.beans;

public class MarketUserBehavior {

    private Long userId;
    private String channel;
    private String behavior;
    private Long timestamp;

    public MarketUserBehavior() {
    }

    public MarketUserBehavior(Long userId, String channel, String behavior, Long timestamp) {
        this.userId = userId;
        this.channel = channel;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "MarketUserBehavior{" +
                "userId=" + userId +
                ", channel='" + channel + '\'' +
                ", behavior='" + behavior + '\'' +
                ", timestamp='" + timestamp + '\'' +
                '}';
    }
}
