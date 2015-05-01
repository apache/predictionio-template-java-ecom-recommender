package org.template.recommendation;

import java.io.Serializable;
import java.util.Map;

public class User implements Serializable {
    private final String entityId;
    private final Map<String, String> properties;

    public User(String entityId, Map<String, String> properties) {
        this.entityId = entityId;
        this.properties = properties;
    }

    public String getEntityId() {
        return entityId;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return "User{" +
                "entityId='" + entityId + '\'' +
                ", properties=" + properties +
                '}';
    }
}
