package org.template.recommendation;

import io.prediction.controller.SanityCheck;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public class TrainingData implements Serializable, SanityCheck {
    private final JavaPairRDD<String, User> users;
    private final JavaPairRDD<String, Item> items;
    private final JavaRDD<UserItemEvent> viewEvents;
    private final JavaRDD<UserItemEvent> buyEvents;

    public TrainingData(JavaPairRDD<String, User> users, JavaPairRDD<String, Item> items, JavaRDD<UserItemEvent> viewEvents, JavaRDD<UserItemEvent> buyEvents) {
        this.users = users;
        this.items = items;
        this.viewEvents = viewEvents;
        this.buyEvents = buyEvents;
    }

    public JavaPairRDD<String, User> getUsers() {
        return users;
    }

    public JavaPairRDD<String, Item> getItems() {
        return items;
    }

    public JavaRDD<UserItemEvent> getViewEvents() {
        return viewEvents;
    }

    public JavaRDD<UserItemEvent> getBuyEvents() {
        return buyEvents;
    }

    @Override
    public void sanityCheck() {
        if (users.isEmpty()) {
            throw new AssertionError("User data is empty");
        }
        if (items.isEmpty()) {
            throw new AssertionError("Item data is empty");
        }
        if (viewEvents.isEmpty()) {
            throw new AssertionError("View Event data is empty");
        }
    }
}
