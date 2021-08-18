package com.jack.beans;

public class UserBehavior {
    // 定义私有属性
    private  Long userId;           // 用户Id
    private Long itemsId;           //商品Id
    private Integer categoryId;     //类别Id
    private String behavior;        // 用户行为
    private Long timeStamp;         // 时间戳

    public UserBehavior() {
    }

    public UserBehavior(Long userId, Long itemsId, Integer categoryId, String behavior, Long timeStamp) {
        this.userId = userId;
        this.itemsId = itemsId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timeStamp = timeStamp;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getItemsId() {
        return itemsId;
    }

    public void setItemsId(Long itemsId) {
        this.itemsId = itemsId;
    }

    public Integer getCategoryId() {
        return categoryId;
    }

    public void setCategoryId(Integer categoryId) {
        this.categoryId = categoryId;
    }

    public String getBehavior() {
        return behavior;
    }

    public void setBehavior(String behavior) {
        this.behavior = behavior;
    }

    public Long getTimeStamp() {
        return timeStamp;
    }

    public void setTimeStamp(Long timeStamp) {
        this.timeStamp = timeStamp;
    }

    @Override
    public String toString() {
        return "UserBehavior{" +
                "userId=" + userId +
                ", itemsId=" + itemsId +
                ", categoryId=" + categoryId +
                ", behavior='" + behavior + '\'' +
                ", timeStamp=" + timeStamp +
                '}';
    }
}
