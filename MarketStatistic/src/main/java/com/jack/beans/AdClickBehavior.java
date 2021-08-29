package com.jack.beans;

public class AdClickBehavior {
    private Long userId;
    private Long AdId;
    private String province;
    private String city;
    private Long timestamp;

    public AdClickBehavior(Long userId, Long adId, String province, String city, Long timestamp) {
        this.userId = userId;
        AdId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }

    public AdClickBehavior() {
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return AdId;
    }

    public void setAdId(Long adId) {
        AdId = adId;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AdClickBehavior{" +
                "userId=" + userId +
                ", AdId=" + AdId +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
