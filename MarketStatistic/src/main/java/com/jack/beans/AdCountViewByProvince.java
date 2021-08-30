package com.jack.beans;

public class AdCountViewByProvince {

    private String Province;
    private String Ts;
    private Long count;

    public AdCountViewByProvince(String province, String ts, Long count) {
        Province = province;
        Ts = ts;
        this.count = count;
    }

    public String getProvince() {
        return Province;
    }

    public void setProvince(String province) {
        Province = province;
    }

    public String getTs() {
        return Ts;
    }

    public void setTs(String ts) {
        Ts = ts;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "AdCountViewByProvince{" +
                "Province='" + Province + '\'' +
                ", Ts='" + Ts + '\'' +
                ", count=" + count +
                '}';
    }
}
