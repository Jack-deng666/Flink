package com.jack.UseCase.CreditCardFraud;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/13 15:36
 */
public class ConsumerMark {
    private String ConsumerInfo;
    private Double Amount;
    private Long Ts;
    private boolean isNormal;

    public String getConsumerInfo() {
        return ConsumerInfo;
    }

    public void setId(String consumerInfo) {
        ConsumerInfo = consumerInfo;
    }

    public Double getAmount() {
        return Amount;
    }

    public void setAmount(Double amount) {
        Amount = amount;
    }

    public Long getTs() {
        return Ts;
    }

    public void setTs(Long ts) {
        Ts = ts;
    }

    public boolean isNormal() {
        return isNormal;
    }

    public void setNormal(boolean normal) {
        isNormal = normal;
    }

    public ConsumerMark() {
    }

    public ConsumerMark(String consumerInfo, Double amount, Long ts, boolean isNormal) {
        ConsumerInfo = consumerInfo;
        Amount = amount;
        Ts = ts;
        this.isNormal = isNormal;
    }

}
