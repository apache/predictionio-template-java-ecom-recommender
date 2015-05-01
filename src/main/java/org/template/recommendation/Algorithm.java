package org.template.recommendation;

import io.prediction.controller.PAlgorithm;
import io.prediction.data.storage.Event;
import io.prediction.data.store.LEventStore;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.jblas.DoubleMatrix;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Algorithm extends PAlgorithm<PreparedData, Model, Query, PredictedResult> {

    private static final Logger logger = LoggerFactory.getLogger(Algorithm.class);
    private final AlgorithmParams ap;

    public Algorithm(AlgorithmParams ap) {
        super(Helper.ofType(Query.class));
        this.ap = ap;
    }

    @Override
    public Model train(SparkContext sc, PreparedData preparedData) {
        TrainingData data = preparedData.getTrainingData();

        // user stuff
        JavaPairRDD<String, Long> userIndexRDD = data.getUsers().map(new Function<Tuple2<String, User>, String>() {
            @Override
            public String call(Tuple2<String, User> idUser) throws Exception {
                return idUser._1();
            }
        }).zipWithIndex();
        final Map<String, Long> userIndexMap = userIndexRDD.collectAsMap();

        // item stuff
        JavaPairRDD<String, Long> itemIndexRDD = data.getItems().map(new Function<Tuple2<String, Item>, String>() {
            @Override
            public String call(Tuple2<String, Item> idItem) throws Exception {
                return idItem._1();
            }
        }).zipWithIndex();
        final Map<String, Long> itemIndexMap = itemIndexRDD.collectAsMap();
        final Map<Long, String> indexItemMap = itemIndexRDD.mapToPair(new PairFunction<Tuple2<String, Long>, Long, String>() {
            @Override
            public Tuple2<Long, String> call(Tuple2<String, Long> element) throws Exception {
                return element.swap();
            }
        }).collectAsMap();

        // ratings stuff
        JavaRDD<Rating> ratings = data.getViewEvents().mapToPair(new PairFunction<UserItemEvent, Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Integer> call(UserItemEvent viewEvent) throws Exception {
                Long userIndex = userIndexMap.get(viewEvent.getUser());
                Long itemIndex = itemIndexMap.get(viewEvent.getItem());

                return (userIndex == null || itemIndex == null) ? null : new Tuple2<>(new Tuple2<>(userIndex.intValue(), itemIndex.intValue()), 1);
            }
        }).filter(new Function<Tuple2<Tuple2<Integer, Integer>, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple2<Integer, Integer>, Integer> element) throws Exception {
                return (element != null);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).map(new Function<Tuple2<Tuple2<Integer, Integer>, Integer>, Rating>() {
            @Override
            public Rating call(Tuple2<Tuple2<Integer, Integer>, Integer> userItemCount) throws Exception {
                return new Rating(userItemCount._1()._1(), userItemCount._1()._2(), userItemCount._2().doubleValue());
            }
        });

        if (ratings.isEmpty())
            throw new AssertionError("Please check if your events contain valid user and item ID.");

        // MLlib ALS stuff
        MatrixFactorizationModel matrixFactorizationModel = ALS.trainImplicit(JavaRDD.toRDD(ratings), ap.getRank(), ap.getIteration(), ap.getLambda(), -1, 1.0, ap.getSeed());
        Map<Object, double[]> userFeatures = JavaPairRDD.fromJavaRDD(matrixFactorizationModel.userFeatures().toJavaRDD()).collectAsMap();
        Map<Object, double[]> productFeatures = JavaPairRDD.fromJavaRDD(matrixFactorizationModel.productFeatures().toJavaRDD()).collectAsMap();

        // popularity scores
        JavaRDD<ItemScore> itemPopularityScore = data.getBuyEvents().mapToPair(new PairFunction<UserItemEvent, Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Integer> call(UserItemEvent buyEvent) throws Exception {
                Long userIndex = userIndexMap.get(buyEvent.getUser());
                Long itemIndex = itemIndexMap.get(buyEvent.getItem());

                return (userIndex == null || itemIndex == null) ? null : new Tuple2<>(new Tuple2<>(userIndex.intValue(), itemIndex.intValue()), 1);
            }
        }).filter(new Function<Tuple2<Tuple2<Integer, Integer>, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<Tuple2<Integer, Integer>, Integer> element) throws Exception {
                return (element != null);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).map(new Function<Tuple2<Tuple2<Integer, Integer>, Integer>, ItemScore>() {
            @Override
            public ItemScore call(Tuple2<Tuple2<Integer, Integer>, Integer> element) throws Exception {
                return new ItemScore(indexItemMap.get(element._1()._2().longValue()), element._2().doubleValue());
            }
        });

        return new Model(userFeatures, productFeatures, userIndexRDD, indexItemMap, itemIndexRDD, itemPopularityScore);
    }

    @Override
    public PredictedResult predict(Model model, final Query query) {
        JavaPairRDD<String, Long> matchedUser = model.getUserIndexRDD().filter(new Function<Tuple2<String, Long>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Long> userIndex) throws Exception {
                return userIndex._1().equals(query.getUserEntityId());
            }
        });

        double[] userFeature = null;
        if (!matchedUser.isEmpty()) {
            userFeature = model.getUserFeatures().get(matchedUser.first()._2());
        }

        if (userFeature != null) {
            return new PredictedResult(topItemsForUser(userFeature, model.getProductFeatures(), model.getIndexItemMap(), query.getNumber()));
        } else {
            List<double[]> recentProductFeatures = getRecentProductFeatures(query, model.getProductFeatures(), model.getItemIndexRDD());
            if (recentProductFeatures.isEmpty()) {
                return new PredictedResult(mostPopularItems(model.getItemPopularityScore(), query.getNumber()));
            } else {
                return new PredictedResult(similarItems(recentProductFeatures, model.getProductFeatures(), model.getIndexItemMap(), query.getNumber()));
            }
        }
    }

    private List<double[]> getRecentProductFeatures(Query query, Map<Object, double[]> productFeatures, JavaPairRDD<String, Long> itemIndexRDD) {
        try {
            List<double[]> result = new ArrayList<>();

            List<Event> events = JavaConversions.asJavaList(LEventStore.findByEntity(
                    ap.getAppName(),
                    "user",
                    query.getUserEntityId(),
                    Option.apply((String) null),
                    Option.apply(JavaConversions.asScalaIterable(ap.getSimilarItemEvents()).toSeq()),
                    Option.apply(Option.apply("item")),
                    Option.apply((Option<String>) null),
                    Option.apply((DateTime) null),
                    Option.apply((DateTime) null),
                    Option.apply((Object) 10),
                    true,
                    Duration.apply(200, TimeUnit.MILLISECONDS)
            ).toSeq());

            for (final Event event : events) {
                if (event.targetEntityId().isDefined()) {
                    JavaPairRDD<String, Long> filtered = itemIndexRDD.filter(new Function<Tuple2<String, Long>, Boolean>() {
                        @Override
                        public Boolean call(Tuple2<String, Long> element) throws Exception {
                            return element._1().equals(event.targetEntityId().get());
                        }
                    });

                    if (!filtered.isEmpty()) {
                        result.add(productFeatures.get(filtered.first()._2().intValue()));
                    }
                }
            }

            return result;
        } catch (Exception e) {
            logger.error("Error reading recent events for user " + query.getUserEntityId());
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private List<ItemScore> topItemsForUser(double[] userFeature, Map<Object, double[]> productFeatures, Map<Long, String> indexItemMap, int number) {
        DoubleMatrix userMatrix = new DoubleMatrix(userFeature);
        Set<Object> keys = productFeatures.keySet();
        List<ItemScore> itemScores = new ArrayList<>(productFeatures.size());
        for (Object key : keys) {
            long longKey = ((Integer) key).longValue();
            double score = userMatrix.dot(new DoubleMatrix(productFeatures.get(key)));
            itemScores.add(new ItemScore(indexItemMap.get(longKey), score));
        }

        Collections.sort(itemScores, Collections.reverseOrder());

        return itemScores.subList(0, Math.min(number, itemScores.size()));
    }

    private List<ItemScore> similarItems(List<double[]> recentProductFeatures, Map<Object, double[]> productFeatures, Map<Long, String> indexItemMap, int number) {
        Set<Object> productKeys = productFeatures.keySet();
        List<ItemScore> itemScores = new ArrayList<>(productFeatures.size());
        for (Object key : productKeys) {
            long longKey = ((Integer) key).longValue();
            double[] feature = productFeatures.get(key);
            double similarity = 0.0;
            for (double[] recentFeature : recentProductFeatures) {
                similarity += cosineSimilarity(feature, recentFeature);
            }
            itemScores.add(new ItemScore(indexItemMap.get(longKey), similarity));
        }

        Collections.sort(itemScores, Collections.reverseOrder());

        return itemScores.subList(0, Math.min(number, itemScores.size()));
    }

    private List<ItemScore> mostPopularItems(JavaRDD<ItemScore> itemPopularityScore, int number) {
        return itemPopularityScore.sortBy(new Function<ItemScore, Double>() {
            @Override
            public Double call(ItemScore itemScore) throws Exception {
                return itemScore.getScore();
            }
        }, false, itemPopularityScore.partitions().size()).take(number);

    }

    private double cosineSimilarity(double[] a, double[] b) {
        DoubleMatrix matrixA = new DoubleMatrix(a);
        DoubleMatrix matrixB = new DoubleMatrix(b);

        return matrixA.dot(matrixB) / (matrixA.norm2() * matrixB.norm2());
    }

}
