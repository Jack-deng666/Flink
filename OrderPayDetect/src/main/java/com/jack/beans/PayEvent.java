package com.jack.beans;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/9/1 10:38
 */
public class PayEvent {
    private String txId;
    private String eventType;
    private Long TimeStamp;

    public PayEvent() {
    }

    public PayEvent(String txId, String eventType, Long timeStamp) {
        this.txId = txId;
        this.eventType = eventType;
        TimeStamp = timeStamp;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Long getTimeStamp() {
        return TimeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        TimeStamp = timeStamp;
    }


    @Override
    public String toString() {
        return "PayEvent{" +
                "txId='" + txId + '\'' +
                ", eventType='" + eventType + '\'' +
                ", TimeStamp=" + TimeStamp +
                '}';
    }
}
