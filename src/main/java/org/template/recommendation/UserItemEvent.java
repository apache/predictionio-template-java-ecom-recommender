package org.template.recommendation;

import java.io.Serializable;

public class UserItemEvent implements Serializable {
    private final String user;
    private final String item;
    private final long time;
    private final UserItemEventType type;

    public UserItemEvent(String user, String item, long time, UserItemEventType type) {
        this.user = user;
        this.item = item;
        this.time = time;
        this.type = type;
    }

    public String getUser() {
        return user;
    }

    public String getItem() {
        return item;
    }

    public long getTime() {
        return time;
    }

    public UserItemEventType getType() {
        return type;
    }

    @Override
    public String toString() {
        return "UserItemEvent{" +
                "user='" + user + '\'' +
                ", item='" + item + '\'' +
                ", time=" + time +
                ", type=" + type +
                '}';
    }
}
