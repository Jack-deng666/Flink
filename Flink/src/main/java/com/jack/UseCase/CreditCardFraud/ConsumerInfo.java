package com.jack.UseCase.CreditCardFraud;

import java.util.TreeSet;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/13 15:00
 */
public class ConsumerInfo {
    public String ConsumerId;
    public Long Ts;
    public Double Amount;


    public String getConsumerId() {
        return ConsumerId;
    }

    public void setConsumerId(String id) {
        ConsumerId = id;
    }

    public Double getAmount() {
        return Amount;
    }

    public void setAmount(Double amount) {
        Amount = amount;
    }

    public ConsumerInfo() {
    }

    @Override
    public String toString() {
        return "ConsumerInfo{" +
                "ConsumerId='" + ConsumerId + '\'' +
                ", Amount=" + Amount +
                ", Ts=" + Ts +
                '}';
    }

    public Long getTs() {
        return Ts;
    }

    public void setTs(Long ts) {
        Ts = ts;
    }

    public ConsumerInfo(String consumerId, Long ts, Double amount) {
        ConsumerId = consumerId;
        Amount = amount;
        Ts = ts;
    }
}
