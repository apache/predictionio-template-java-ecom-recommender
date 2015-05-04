package org.template.recommendation;

import io.prediction.controller.Params;
import io.prediction.controller.PersistentModel;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Map;

public class Model implements Serializable, PersistentModel<AlgorithmParams> {
    private static final Logger logger = LoggerFactory.getLogger(Model.class);
    private final Map<Object, double[]> userFeatures;
    private final Map<Object, double[]> productFeatures;
    private final JavaPairRDD<String, Long> userIndexRDD;
    private final Map<Long, String> indexItemMap;
    private final JavaPairRDD<String, Long> itemIndexRDD;
    private final JavaRDD<ItemScore> itemPopularityScore;
    private final JavaPairRDD<String, Item> items;

    public Model(Map<Object, double[]> userFeatures, Map<Object, double[]> productFeatures, JavaPairRDD<String, Long> userIndexRDD, Map<Long, String> itemIndexMap, JavaPairRDD<String, Long> itemIndexRDD, JavaRDD<ItemScore> itemPopularityScore, JavaPairRDD<String, Item> items) {
        this.userFeatures = userFeatures;
        this.productFeatures = productFeatures;
        this.userIndexRDD = userIndexRDD;
        this.indexItemMap = itemIndexMap;
        this.itemIndexRDD = itemIndexRDD;
        this.itemPopularityScore = itemPopularityScore;
        this.items = items;
    }

    public Map<Object, double[]> getUserFeatures() {
        return userFeatures;
    }

    public Map<Object, double[]> getProductFeatures() {
        return productFeatures;
    }

    public JavaPairRDD<String, Long> getUserIndexRDD() {
        return userIndexRDD;
    }

    public Map<Long, String> getIndexItemMap() {
        return indexItemMap;
    }

    public JavaPairRDD<String, Long> getItemIndexRDD() {
        return itemIndexRDD;
    }

    public JavaRDD<ItemScore> getItemPopularityScore() {
        return itemPopularityScore;
    }

    public JavaPairRDD<String, Item> getItems() {
        return items;
    }

    @Override
    public boolean save(String id, AlgorithmParams params, SparkContext sc) {

        logger.info("saved model");
        return false;
    }

    public static Model load(String id, Params params, SparkContext sc) {

        logger.info("loaded model");
        return null;
    }
}
