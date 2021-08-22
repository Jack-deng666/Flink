package com.jack.beans;

public class HotPageCount {
    private String url;
    private Long windowEnd;
    private Integer count;

    public HotPageCount() {
    }

    public HotPageCount(String url, Long timestamp, Integer count) {
        this.url = url;
        this.windowEnd = timestamp;
        this.count = count;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public Long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(Long timestamp) {
        this.windowEnd = timestamp;
    }

    public Integer getCount() {
        return count;
    }

    public void setCount(Integer count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "HotPageCount{" +
                "url='" + url + '\'' +
                ", windowEnd=" + windowEnd +
                ", count=" + count +
                '}';
    }
}
