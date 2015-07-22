package org.template.recommendation;

import io.prediction.controller.Params;
import io.prediction.controller.PersistentModel;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

public class Model implements Serializable, PersistentModel<AlgorithmParams> {
    private static final Logger logger = LoggerFactory.getLogger(Model.class);
    private final JavaPairRDD<Integer, double[]> userFeatures;
    private final JavaPairRDD<Integer, Tuple2<String, double[]>> indexItemFeatures;
    private final JavaPairRDD<String, Integer> userIndex;
    private final JavaPairRDD<String, Integer> itemIndex;
    private final JavaRDD<ItemScore> itemPopularityScore;
    private final Map<String, Item> items;

    public Model(JavaPairRDD<Integer, double[]> userFeatures, JavaPairRDD<Integer, Tuple2<String, double[]>> indexItemFeatures, JavaPairRDD<String, Integer> userIndex, JavaPairRDD<String, Integer> itemIndex, JavaRDD<ItemScore> itemPopularityScore, Map<String, Item> items) {
        this.userFeatures = userFeatures;
        this.indexItemFeatures = indexItemFeatures;
        this.userIndex = userIndex;
        this.itemIndex = itemIndex;
        this.itemPopularityScore = itemPopularityScore;
        this.items = items;
    }

    public JavaPairRDD<Integer, double[]> getUserFeatures() {
        return userFeatures;
    }

    public JavaPairRDD<Integer, Tuple2<String, double[]>> getIndexItemFeatures() {
        return indexItemFeatures;
    }

    public JavaPairRDD<String, Integer> getUserIndex() {
        return userIndex;
    }

    public JavaPairRDD<String, Integer> getItemIndex() {
        return itemIndex;
    }

    public JavaRDD<ItemScore> getItemPopularityScore() {
        return itemPopularityScore;
    }

    public Map<String, Item> getItems() {
        return items;
    }

    @Override
    public boolean save(String id, AlgorithmParams params, SparkContext sc) {
        userFeatures.saveAsObjectFile("/tmp/" + id + "/userFeatures");
        indexItemFeatures.saveAsObjectFile("/tmp/" + id + "/indexItemFeatures");
        userIndex.saveAsObjectFile("/tmp/" + id + "/userIndex");
        itemIndex.saveAsObjectFile("/tmp/" + id + "/itemIndex");
        itemPopularityScore.saveAsObjectFile("/tmp/" + id + "/itemPopularityScore");
        new JavaSparkContext(sc).parallelize(Collections.singletonList(items)).saveAsObjectFile("/tmp/" + id + "/items");

        logger.info("Saved model to /tmp/" + id);
        return true;
    }

    public static Model load(String id, Params params, SparkContext sc) {
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);
        JavaPairRDD<Integer, double[]> userFeatures = JavaPairRDD.<Integer, double[]>fromJavaRDD(jsc.<Tuple2<Integer, double[]>>objectFile("/tmp/" + id + "/userFeatures"));
        JavaPairRDD<Integer, Tuple2<String, double[]>> indexItemFeatures = JavaPairRDD.<Integer, Tuple2<String, double[]>>fromJavaRDD(jsc.<Tuple2<Integer, Tuple2<String, double[]>>>objectFile("/tmp/" + id + "/indexItemFeatures"));
        JavaPairRDD<String, Integer> userIndex = JavaPairRDD.<String, Integer>fromJavaRDD(jsc.<Tuple2<String, Integer>>objectFile("/tmp/" + id + "/userIndex"));
        JavaPairRDD<String, Integer> itemIndex = JavaPairRDD.<String, Integer>fromJavaRDD(jsc.<Tuple2<String, Integer>>objectFile("/tmp/" + id + "/itemIndex"));
        JavaRDD<ItemScore> itemPopularityScore = jsc.objectFile("/tmp/" + id + "/itemPopularityScore");
        Map<String, Item> items = jsc.<Map<String, Item>>objectFile("/tmp/" + id + "/items").collect().get(0);

        logger.info("loaded model");
        return new Model(userFeatures, indexItemFeatures, userIndex, itemIndex, itemPopularityScore, items);
    }
}
