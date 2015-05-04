package org.template.recommendation;

import com.google.common.collect.Sets;
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
        JavaPairRDD<String, Integer> userIndexRDD = data.getUsers().map(new Function<Tuple2<String, User>, String>() {
            @Override
            public String call(Tuple2<String, User> idUser) throws Exception {
                return idUser._1();
            }
        }).zipWithIndex().mapToPair(new PairFunction<Tuple2<String, Long>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Long> element) throws Exception {
                return new Tuple2<>(element._1(), element._2().intValue());
            }
        });
        final Map<String, Integer> userIndexMap = userIndexRDD.collectAsMap();

        // item stuff
        JavaPairRDD<String, Integer> itemIndexRDD = data.getItems().map(new Function<Tuple2<String, Item>, String>() {
            @Override
            public String call(Tuple2<String, Item> idItem) throws Exception {
                return idItem._1();
            }
        }).zipWithIndex().mapToPair(new PairFunction<Tuple2<String, Long>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Long> element) throws Exception {
                return new Tuple2<>(element._1(), element._2().intValue());
            }
        });
        final Map<String, Integer> itemIndexMap = itemIndexRDD.collectAsMap();
        JavaPairRDD<Integer, String> indexItemRDD = itemIndexRDD.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> element) throws Exception {
                return element.swap();
            }
        });
        final Map<Integer, String> indexItemMap = indexItemRDD.collectAsMap();

        // ratings stuff
        JavaRDD<Rating> ratings = data.getViewEvents().mapToPair(new PairFunction<UserItemEvent, Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Integer> call(UserItemEvent viewEvent) throws Exception {
                Integer userIndex = userIndexMap.get(viewEvent.getUser());
                Integer itemIndex = itemIndexMap.get(viewEvent.getItem());

                return (userIndex == null || itemIndex == null) ? null : new Tuple2<>(new Tuple2<>(userIndex, itemIndex), 1);
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
        JavaPairRDD<Integer, double[]> userFeatures = matrixFactorizationModel.userFeatures().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, double[]>, Integer, double[]>() {
            @Override
            public Tuple2<Integer, double[]> call(Tuple2<Object, double[]> element) throws Exception {
                return new Tuple2<>((Integer) element._1(), element._2());
            }
        });
        JavaPairRDD<Integer, double[]> productFeaturesRDD = matrixFactorizationModel.productFeatures().toJavaRDD().mapToPair(new PairFunction<Tuple2<Object, double[]>, Integer, double[]>() {
            @Override
            public Tuple2<Integer, double[]> call(Tuple2<Object, double[]> element) throws Exception {
                return new Tuple2<>((Integer) element._1(), element._2());
            }
        });

        // popularity scores
        JavaRDD<ItemScore> itemPopularityScore = data.getBuyEvents().mapToPair(new PairFunction<UserItemEvent, Tuple2<Integer, Integer>, Integer>() {
            @Override
            public Tuple2<Tuple2<Integer, Integer>, Integer> call(UserItemEvent buyEvent) throws Exception {
                Integer userIndex = userIndexMap.get(buyEvent.getUser());
                Integer itemIndex = itemIndexMap.get(buyEvent.getItem());

                return (userIndex == null || itemIndex == null) ? null : new Tuple2<>(new Tuple2<>(userIndex, itemIndex), 1);
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
                return new ItemScore(indexItemMap.get(element._1()._2()), element._2().doubleValue());
            }
        });

        JavaPairRDD<Integer, Tuple2<String, double[]>> indexItemFeatures = indexItemRDD.join(productFeaturesRDD);

        return new Model(userFeatures, indexItemFeatures, userIndexRDD, itemIndexRDD, itemPopularityScore, data.getItems());
    }

    @Override
    public PredictedResult predict(Model model, final Query query) {
        final JavaPairRDD<String, Integer> matchedUser = model.getUserIndex().filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> userIndex) throws Exception {
                return userIndex._1().equals(query.getUserEntityId());
            }
        });

        double[] userFeature = null;
        if (!matchedUser.isEmpty()) {
            final Integer matchedUserIndex = matchedUser.first()._2();
            userFeature = model.getUserFeatures().filter(new Function<Tuple2<Integer, double[]>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Integer, double[]> element) throws Exception {
                    return element._1().equals(matchedUserIndex);
                }
            }).first()._2();
        }

        if (userFeature != null) {
            return new PredictedResult(topItemsForUser(userFeature, model, query));
        } else {
            List<double[]> recentProductFeatures = getRecentProductFeatures(query, model);
            if (recentProductFeatures.isEmpty()) {
                return new PredictedResult(mostPopularItems(model, query));
            } else {
                return new PredictedResult(similarItems(recentProductFeatures, model, query));
            }
        }
    }

    private List<double[]> getRecentProductFeatures(Query query, Model model) {
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
                    JavaPairRDD<String, Integer> filtered = model.getItemIndex().filter(new Function<Tuple2<String, Integer>, Boolean>() {
                        @Override
                        public Boolean call(Tuple2<String, Integer> element) throws Exception {
                            return element._1().equals(event.targetEntityId().get());
                        }
                    });

                    final Integer itemIndex = filtered.first()._2();

                    if (!filtered.isEmpty()) {
                        result.add(model.getIndexItemFeatures().filter(new Function<Tuple2<Integer, Tuple2<String, double[]>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<Integer, Tuple2<String, double[]>> element) throws Exception {
                                return itemIndex.equals(element._1());
                            }
                        }).first()._2()._2());
                    }
                }
            }

            return result;
        } catch (Exception e) {
            logger.error("Error reading recent events for user " + query.getUserEntityId());
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private List<ItemScore> topItemsForUser(double[] userFeature, Model model, Query query) {
        final DoubleMatrix userMatrix = new DoubleMatrix(userFeature);

        List<ItemScore> itemScores = model.getIndexItemFeatures().map(new Function<Tuple2<Integer, Tuple2<String, double[]>>, ItemScore>() {
            @Override
            public ItemScore call(Tuple2<Integer, Tuple2<String, double[]>> element) throws Exception {
                return new ItemScore(element._2()._1(), userMatrix.dot(new DoubleMatrix(element._2()._2())));
            }
        }).collect();

        itemScores = validScores(itemScores, query.getWhitelist(), query.getBlacklist(), query.getCategories(), model.getItems());
        Collections.sort(itemScores, Collections.reverseOrder());

        return itemScores.subList(0, Math.min(query.getNumber(), itemScores.size()));
    }

    private List<ItemScore> similarItems(final List<double[]> recentProductFeatures, Model model, Query query) {
        List<ItemScore> itemScores = model.getIndexItemFeatures().map(new Function<Tuple2<Integer, Tuple2<String, double[]>>, ItemScore>() {
            @Override
            public ItemScore call(Tuple2<Integer, Tuple2<String, double[]>> element) throws Exception {
                double similarity = 0.0;
                for (double[] recentFeature : recentProductFeatures) {
                    similarity += cosineSimilarity(element._2()._2(), recentFeature);
                }

                return new ItemScore(element._2()._1(), similarity);
            }
        }).collect();

        itemScores = validScores(itemScores, query.getWhitelist(), query.getBlacklist(), query.getCategories(), model.getItems());
        Collections.sort(itemScores, Collections.reverseOrder());

        return itemScores.subList(0, Math.min(query.getNumber(), itemScores.size()));
    }

    private List<ItemScore> mostPopularItems(Model model, Query query) {
        List<ItemScore> itemScores = validScores(model.getItemPopularityScore().collect(), query.getWhitelist(), query.getBlacklist(), query.getCategories(), model.getItems());
        Collections.sort(itemScores, Collections.reverseOrder());

        return itemScores.subList(0, Math.min(query.getNumber(), itemScores.size()));
    }

    private double cosineSimilarity(double[] a, double[] b) {
        DoubleMatrix matrixA = new DoubleMatrix(a);
        DoubleMatrix matrixB = new DoubleMatrix(b);

        return matrixA.dot(matrixB) / (matrixA.norm2() * matrixB.norm2());
    }

    private List<ItemScore> validScores(List<ItemScore> all, Set<String> whitelist, Set<String> blacklist, Set<String> categories, JavaPairRDD<String, Item> items) {
        if (whitelist.isEmpty() && blacklist.isEmpty() && categories.isEmpty()) return all;

        List<ItemScore> result = new ArrayList<>();
        for (final ItemScore itemScore : all) {
            JavaPairRDD<String, Item> possibleItems = items.filter(new Function<Tuple2<String, Item>, Boolean>() {
                @Override
                public Boolean call(Tuple2<String, Item> element) throws Exception {
                    return element._1().equals(itemScore.getItemEntityId());
                }
            });

            if (!possibleItems.isEmpty()) {
                Item item = possibleItems.first()._2();
                if ((whitelist.isEmpty() || whitelist.contains(itemScore.getItemEntityId())) &&
                        !blacklist.contains(itemScore.getItemEntityId()) &&
                        (categories.isEmpty() || Sets.intersection(categories, item.getCategories()).size() > 0)) {
                    result.add(itemScore);
                }
            }
        }

        return result;
    }
}
