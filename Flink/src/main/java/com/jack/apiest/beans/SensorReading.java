package com.jack.apiest.beans;


// 擦混感器温度读书的数据类型
public class SensorReading {
    private String id;
    private  Long timestamp;
    private Double tempperature;

    public SensorReading() {
    }

    public SensorReading(String id, Long timestamp, Double tempperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.tempperature = tempperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Double getTempperature() {
        return tempperature;
    }

    public void setTempperature(Double tempperature) {
        this.tempperature = tempperature;
    }

    @Override
    public String toString() {
        return "SensorReading{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", tempperature=" + tempperature +
                '}';
    }
}
