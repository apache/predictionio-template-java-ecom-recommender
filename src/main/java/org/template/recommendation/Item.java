package org.template.recommendation;

import java.io.Serializable;
import java.util.List;

public class Item implements Serializable{
    private final List<String> categories;
    private final String entityId;

    public Item(String entityId, List<String> categories) {
        this.categories = categories;
        this.entityId = entityId;
    }

    public String getEntityId() {
        return entityId;
    }

    public List<String> getCategories() {
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
