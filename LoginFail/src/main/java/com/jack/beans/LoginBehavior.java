package com.jack.beans;

/**
 * @author jack Deng
 * @version 1.0
 * @date 2021/8/31 15:38
 */
public class LoginBehavior {
    private Long UserId;
    private String IP;
    private String LoginState;
    private Long timestamp;

    public LoginBehavior(Long userId, String IP, String loginState, Long timestamp) {
        UserId = userId;
        this.IP = IP;
        LoginState = loginState;
        this.timestamp = timestamp;
    }

    public LoginBehavior() {
    }

    public Long getUserId() {
        return UserId;
    }

    public void setUserId(Long userId) {
        UserId = userId;
    }

    public String getIP() {
        return IP;
    }

    public void setIP(String IP) {
        this.IP = IP;
    }

    public String getLoginState() {
        return LoginState;
    }

    public void setLoginState(String loginState) {
        LoginState = loginState;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginBehavior{" +
                "UserId=" + UserId +
                ", IP='" + IP + '\'' +
                ", LoginState='" + LoginState + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
