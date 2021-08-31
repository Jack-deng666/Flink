package com.jack.beans;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/31 15:47
 */
public class LoginFailBehavior {
    private Long UserId;
    private Long firstLoginTime;
    private Long lastLoginTime;
    private String warningMessage;

    public LoginFailBehavior() {
    }

    public LoginFailBehavior(Long userId, Long firstLoginTime, Long lastLoginTime, String warningMessage) {
        UserId = userId;
        this.firstLoginTime = firstLoginTime;
        this.lastLoginTime = lastLoginTime;
        this.warningMessage = warningMessage;
    }

    public Long getUserId() {
        return UserId;
    }

    public void setUserId(Long userId) {
        UserId = userId;
    }

    public Long getFirstLoginTime() {
        return firstLoginTime;
    }

    public void setFirstLoginTime(Long firstLoginTime) {
        this.firstLoginTime = firstLoginTime;
    }

    public Long getLastLoginTime() {
        return lastLoginTime;
    }

    public void setLastLoginTime(Long lastLoginTime) {
        this.lastLoginTime = lastLoginTime;
    }

    public String getWarningMessage() {
        return warningMessage;
    }

    public void setWarningMessage(String warningMessage) {
        this.warningMessage = warningMessage;
    }

    @Override
    public String toString() {
        return "LoginFailBehavior{" +
                "UserId=" + UserId +
                ", firstLoginTime=" + firstLoginTime +
                ", lastLoginTime=" + lastLoginTime +
                ", warningMessage='" + warningMessage + '\'' +
                '}';
    }
}
