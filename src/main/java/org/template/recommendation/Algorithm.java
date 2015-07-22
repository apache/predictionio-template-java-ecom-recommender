package org.template.recommendation;

import com.google.common.collect.Sets;
import io.prediction.controller.java.PJavaAlgorithm;
import io.prediction.data.storage.Event;
import io.prediction.data.store.java.LJavaEventStore;
import io.prediction.data.store.java.OptionHelper;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.jblas.DoubleMatrix;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.Tuple2;
import scala.concurrent.duration.Duration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class Algorithm extends PJavaAlgorithm<PreparedData, Model, Query, PredictedResult> {

    private static final Logger logger = LoggerFactory.getLogger(Algorithm.class);
    private final AlgorithmParams ap;

    public Algorithm(AlgorithmParams ap) {
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
        }).mapToPair(new PairFunction<Tuple2<Tuple2<Integer, Integer>, Integer>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Tuple2<Integer, Integer>, Integer> element) throws Exception {
                return new Tuple2<>(element._1()._2(), element._2());
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }).map(new Function<Tuple2<Integer, Integer>, ItemScore>() {
            @Override
            public ItemScore call(Tuple2<Integer, Integer> element) throws Exception {
                return new ItemScore(indexItemMap.get(element._1()), element._2().doubleValue());
            }
        });

        JavaPairRDD<Integer, Tuple2<String, double[]>> indexItemFeatures = indexItemRDD.join(productFeaturesRDD);

        return new Model(userFeatures, indexItemFeatures, userIndexRDD, itemIndexRDD, itemPopularityScore, data.getItems().collectAsMap());
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

    @Override
    public RDD<Tuple2<Object, PredictedResult>> batchPredict(Model model, RDD<Tuple2<Object, Query>> qs) {
        List<Tuple2<Object, Query>> indexQueries = qs.toJavaRDD().collect();
        List<Tuple2<Object, PredictedResult>> results = new ArrayList<>();

        for (Tuple2<Object, Query> indexQuery : indexQueries) {
            results.add(new Tuple2<>(indexQuery._1(), predict(model, indexQuery._2())));
        }

        return new JavaSparkContext(qs.sparkContext()).parallelize(results).rdd();
    }

    private List<double[]> getRecentProductFeatures(Query query, Model model) {
        try {
            List<double[]> result = new ArrayList<>();

            List<Event> events = LJavaEventStore.findByEntity(
                    ap.getAppName(),
                    "user",
                    query.getUserEntityId(),
                    OptionHelper.<String>none(),
                    OptionHelper.some(ap.getSimilarItemEvents()),
                    OptionHelper.some(OptionHelper.some("item")),
                    OptionHelper.<Option<String>>none(),
                    OptionHelper.<DateTime>none(),
                    OptionHelper.<DateTime>none(),
                    OptionHelper.some(10),
                    true,
                    Duration.apply(10, TimeUnit.SECONDS));

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

                        JavaPairRDD<Integer, Tuple2<String, double[]>> indexItemFeatures = model.getIndexItemFeatures().filter(new Function<Tuple2<Integer, Tuple2<String, double[]>>, Boolean>() {
                            @Override
                            public Boolean call(Tuple2<Integer, Tuple2<String, double[]>> element) throws Exception {
                                return itemIndex.equals(element._1());
                            }
                        });

                        List<Tuple2<Integer, Tuple2<String, double[]>>> oneIndexItemFeatures = indexItemFeatures.collect();
                        if (oneIndexItemFeatures.size() > 0) {
                            result.add(oneIndexItemFeatures.get(0)._2()._2());
                        }
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

        JavaRDD<ItemScore> itemScores = model.getIndexItemFeatures().map(new Function<Tuple2<Integer, Tuple2<String, double[]>>, ItemScore>() {
            @Override
            public ItemScore call(Tuple2<Integer, Tuple2<String, double[]>> element) throws Exception {
                return new ItemScore(element._2()._1(), userMatrix.dot(new DoubleMatrix(element._2()._2())));
            }
        });

        itemScores = validScores(itemScores, query.getWhitelist(), query.getBlacklist(), query.getCategories(), model.getItems(), query.getUserEntityId());
        return sortAndTake(itemScores, query.getNumber());
    }

    private List<ItemScore> similarItems(final List<double[]> recentProductFeatures, Model model, Query query) {
        JavaRDD<ItemScore> itemScores = model.getIndexItemFeatures().map(new Function<Tuple2<Integer, Tuple2<String, double[]>>, ItemScore>() {
            @Override
            public ItemScore call(Tuple2<Integer, Tuple2<String, double[]>> element) throws Exception {
                double similarity = 0.0;
                for (double[] recentFeature : recentProductFeatures) {
                    similarity += cosineSimilarity(element._2()._2(), recentFeature);
                }

                return new ItemScore(element._2()._1(), similarity);
            }
        });

        itemScores = validScores(itemScores, query.getWhitelist(), query.getBlacklist(), query.getCategories(), model.getItems(), query.getUserEntityId());
        return sortAndTake(itemScores, query.getNumber());
    }

    private List<ItemScore> mostPopularItems(Model model, Query query) {
        JavaRDD<ItemScore> itemScores = validScores(model.getItemPopularityScore(), query.getWhitelist(), query.getBlacklist(), query.getCategories(), model.getItems(), query.getUserEntityId());
        return sortAndTake(itemScores, query.getNumber());
    }

    private double cosineSimilarity(double[] a, double[] b) {
        DoubleMatrix matrixA = new DoubleMatrix(a);
        DoubleMatrix matrixB = new DoubleMatrix(b);

        return matrixA.dot(matrixB) / (matrixA.norm2() * matrixB.norm2());
    }

    private List<ItemScore> sortAndTake(JavaRDD<ItemScore> all, int number) {
        return all.sortBy(new Function<ItemScore, Double>() {
            @Override
            public Double call(ItemScore itemScore) throws Exception {
                return itemScore.getScore();
            }
        }, false, all.partitions().size()).take(number);
    }

    private JavaRDD<ItemScore> validScores(JavaRDD<ItemScore> all, final Set<String> whitelist, final Set<String> blacklist, final Set<String> categories, final Map<String, Item> items, String userEntityId) {
        final Set<String> seenItemEntityIds = seenItemEntityIds(userEntityId);
        final Set<String> unavailableItemEntityIds = unavailableItemEntityIds();

        return all.filter(new Function<ItemScore, Boolean>() {
            @Override
            public Boolean call(ItemScore itemScore) throws Exception {
                Item item = items.get(itemScore.getItemEntityId());

                return (item != null
                        && passWhitelistCriteria(whitelist, item.getEntityId())
                        && passBlacklistCriteria(blacklist, item.getEntityId())
                        && passCategoryCriteria(categories, item)
                        && passUnseenCriteria(seenItemEntityIds, item.getEntityId())
                        && passAvailabilityCriteria(unavailableItemEntityIds, item.getEntityId()));
            }
        });
    }

    private boolean passWhitelistCriteria(Set<String> whitelist, String itemEntityId) {
        return (whitelist.isEmpty() || whitelist.contains(itemEntityId));
    }

    private boolean passBlacklistCriteria(Set<String> blacklist, String itemEntityId) {
        return !blacklist.contains(itemEntityId);
    }

    private boolean passCategoryCriteria(Set<String> categories, Item item) {
        return (categories.isEmpty() || Sets.intersection(categories, item.getCategories()).size() > 0);
    }

    private boolean passUnseenCriteria(Set<String> seen, String itemEntityId) {
        return !seen.contains(itemEntityId);
    }

    private boolean passAvailabilityCriteria(Set<String> unavailableItemEntityIds, String entityId) {
        return !unavailableItemEntityIds.contains(entityId);
    }

    private Set<String> unavailableItemEntityIds() {
        try {
            List<Event> unavailableConstraintEvents = LJavaEventStore.findByEntity(
                    ap.getAppName(),
                    "constraint",
                    "unavailableItems",
                    OptionHelper.<String>none(),
                    OptionHelper.some(Collections.singletonList("$set")),
                    OptionHelper.<Option<String>>none(),
                    OptionHelper.<Option<String>>none(),
                    OptionHelper.<DateTime>none(),
                    OptionHelper.<DateTime>none(),
                    OptionHelper.some(1),
                    true,
                    Duration.apply(10, TimeUnit.SECONDS));

            if (unavailableConstraintEvents.isEmpty()) return Collections.emptySet();

            Event unavailableConstraint = unavailableConstraintEvents.get(0);

            List<String> unavailableItems = unavailableConstraint.properties().getStringList("items");

            return new HashSet<>(unavailableItems);
        } catch (Exception e) {
            logger.error("Error reading constraint events");
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private Set<String> seenItemEntityIds(String userEntityId) {
        if (!ap.isUnseenOnly()) return Collections.emptySet();

        try {
            Set<String> result = new HashSet<>();
            List<Event> seenEvents = LJavaEventStore.findByEntity(
                    ap.getAppName(),
                    "user",
                    userEntityId,
                    OptionHelper.<String>none(),
                    OptionHelper.some(ap.getSeenItemEvents()),
                    OptionHelper.some(OptionHelper.some("item")),
                    OptionHelper.<Option<String>>none(),
                    OptionHelper.<DateTime>none(),
                    OptionHelper.<DateTime>none(),
                    OptionHelper.<Integer>none(),
                    true,
                    Duration.apply(10, TimeUnit.SECONDS));

            for (Event event : seenEvents) {
                result.add(event.targetEntityId().get());
            }

            return result;
        } catch (Exception e) {
            logger.error("Error reading seen events for user " + userEntityId);
            throw new RuntimeException(e.getMessage(), e);
        }
    }
}
