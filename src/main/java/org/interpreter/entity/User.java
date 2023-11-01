package org.interpreter.entity;

public class User {
    private String username;
    private String phone;
    private String group;

    public User(String username, String phone, String group) {
        this.username = username;
        this.phone = phone;
        this.group = group;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }
}
