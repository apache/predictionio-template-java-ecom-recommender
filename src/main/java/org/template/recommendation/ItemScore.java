package org.template.recommendation;

import java.io.Serializable;

public class ItemScore implements Serializable, Comparable<ItemScore> {
    private final String itemEntityId;
    private final double score;

    public ItemScore(String itemEntityId, double score) {
        this.itemEntityId = itemEntityId;
        this.score = score;
    }

    public String getItemEntityId() {
        return itemEntityId;
    }

    public double getScore() {
        return score;
    }

    @Override
    public String toString() {
        return "ItemScore{" +
                "itemEntityId='" + itemEntityId + '\'' +
                ", score=" + score +
                '}';
    }

    @Override
    public int compareTo(ItemScore o) {
        return Double.valueOf(score).compareTo(o.score);
    }
}
