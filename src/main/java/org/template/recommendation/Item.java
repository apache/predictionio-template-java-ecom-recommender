package org.template.recommendation;

import java.io.Serializable;
import java.util.Set;

public class Item implements Serializable{
    private final Set<String> categories;
    private final String entityId;

    public Item(String entityId, Set<String> categories) {
        this.categories = categories;
        this.entityId = entityId;
    }

    public String getEntityId() {
        return entityId;
    }

    public Set<String> getCategories() {
        return categories;
    }

    @Override
    public String toString() {
        return "Item{" +
                "categories=" + categories +
                ", entityId='" + entityId + '\'' +
                '}';
    }

}
