package org.template.recommendation;

import java.io.Serializable;
import java.util.Set;

public class Query implements Serializable{
    private final String userEntityId;
    private final int number;
    private final Set<String> categories;
    private final Set<String> whitelist;
    private final Set<String> blacklist;

    public Query(String userEntityId, int number, Set<String> categories, Set<String> whitelist, Set<String> blacklist) {
        this.userEntityId = userEntityId;
        this.number = number;
        this.categories = categories;
        this.whitelist = whitelist;
        this.blacklist = blacklist;
    }

    public String getUserEntityId() {
        return userEntityId;
    }

    public int getNumber() {
        return number;
    }

    public Set<String> getCategories() {
        return categories;
    }

    public Set<String> getWhitelist() {
        return whitelist;
    }

    public Set<String> getBlacklist() {
        return blacklist;
    }

    @Override
    public String toString() {
        return "Query{" +
                "userEntityId='" + userEntityId + '\'' +
                ", number=" + number +
                ", categories=" + categories +
                ", whitelist=" + whitelist +
                ", blacklist=" + blacklist +
                '}';
    }
}
