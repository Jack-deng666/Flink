package com.jack.beans;

public class UserView {
    private String ip;
    private String userId;
    private String url;
    private Long timeStamp;
    private String method;

    public UserView() {
    }

    public UserView(String ip, String userId, String url, Long timeStamp, String method) {
        this.ip = ip;
        this.userId = userId;
        this.url = url;
        this.timeStamp = timeStamp;
        this.method = method;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    public String getMethod() {
        return method;
    }

    public void setMethod(String method) {
        this.method = method;
    }

    @Override
    public String toString() {
        return "UserView{" +
                "ip='" + ip + '\'' +
                ", userId='" + userId + '\'' +
                ", url='" + url + '\'' +
                ", timeStamp=" + timeStamp +
                ", method='" + method + '\'' +
                '}';
    }
}
