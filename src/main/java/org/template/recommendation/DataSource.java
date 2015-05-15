package org.template.recommendation;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.prediction.controller.EmptyParams;
import io.prediction.controller.java.PJavaDataSource;
import io.prediction.data.storage.Event;
import io.prediction.data.storage.PropertyMap;
import io.prediction.data.store.java.OptionHelper;
import io.prediction.data.store.java.PJavaEventStore;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;
import org.joda.time.DateTime;
import scala.Option;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConversions;
import scala.collection.JavaConversions$;
import scala.collection.Seq;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataSource extends PJavaDataSource<TrainingData, EmptyParams, Query, Set<String>> {

    private final DataSourceParams dsp;

    public DataSource(DataSourceParams dsp) {
        this.dsp = dsp;
    }

    @Override
    public TrainingData readTraining(SparkContext sc) {
        JavaPairRDD<String,User> usersRDD = PJavaEventStore.aggregateProperties(
                dsp.getAppName(),
                "user",
                OptionHelper.<String>none(),
                OptionHelper.<DateTime>none(),
                OptionHelper.<DateTime>none(),
                OptionHelper.<List<String>>none(),
                sc)
                .mapToPair(new PairFunction<Tuple2<String, PropertyMap>, String, User>() {
                    @Override
                    public Tuple2<String, User> call(Tuple2<String, PropertyMap> entityIdProperty) throws Exception {
                        Set<String> keys = JavaConversions$.MODULE$.setAsJavaSet(entityIdProperty._2().keySet());
                        Map<String, String> properties = new HashMap<>();
                        for (String key : keys) {
                            properties.put(key, entityIdProperty._2().get(key, String.class));
                        }

                        User user = new User(entityIdProperty._1(), ImmutableMap.copyOf(properties));

                        return new Tuple2<>(user.getEntityId(), user);
                    }
                });

        JavaPairRDD<String, Item> itemsRDD = PJavaEventStore.aggregateProperties(
                dsp.getAppName(),
                "item",
                OptionHelper.<String>none(),
                OptionHelper.<DateTime>none(),
                OptionHelper.<DateTime>none(),
                OptionHelper.<List<String>>none(),
                sc)
                .mapToPair(new PairFunction<Tuple2<String, PropertyMap>, String, Item>() {
                    @Override
                    public Tuple2<String, Item> call(Tuple2<String, PropertyMap> entityIdProperty) throws Exception {
                        List<String> categories = entityIdProperty._2().getStringList("categories");
                        Item item = new Item(entityIdProperty._1(), ImmutableSet.copyOf(categories));

                        return new Tuple2<>(item.getEntityId(), item);
                    }
                });

        JavaRDD<UserItemEvent> viewEventsRDD = PJavaEventStore.find(
                dsp.getAppName(),
                OptionHelper.<String>none(),
                OptionHelper.<DateTime>none(),
                OptionHelper.<DateTime>none(),
                OptionHelper.some("user"),
                OptionHelper.<String>none(),
                OptionHelper.some(Collections.singletonList("view")),
                OptionHelper.<Option<String>>none(),
                OptionHelper.<Option<String>>none(),
                sc)
                .map(new Function<Event, UserItemEvent>() {
                    @Override
                    public UserItemEvent call(Event event) throws Exception {
                        return new UserItemEvent(event.entityId(), event.targetEntityId().get(), event.eventTime().getMillis(), UserItemEventType.VIEW);
                    }
                });

        JavaRDD<UserItemEvent> buyEventsRDD = PJavaEventStore.find(
                dsp.getAppName(),
                OptionHelper.<String>none(),
                OptionHelper.<DateTime>none(),
                OptionHelper.<DateTime>none(),
                OptionHelper.some("user"),
                OptionHelper.<String>none(),
                OptionHelper.some(Collections.singletonList("buy")),
                OptionHelper.<Option<String>>none(),
                OptionHelper.<Option<String>>none(),
                sc)
                .map(new Function<Event, UserItemEvent>() {
                    @Override
                    public UserItemEvent call(Event event) throws Exception {
                        return new UserItemEvent(event.entityId(), event.targetEntityId().get(), event.eventTime().getMillis(), UserItemEventType.BUY);
                    }
                });

        return new TrainingData(usersRDD, itemsRDD, viewEventsRDD, buyEventsRDD);
    }

    @Override
    public Seq<Tuple3<TrainingData, EmptyParams, RDD<Tuple2<Query, Set<String>>>>> readEval(SparkContext sc) {
        TrainingData all = readTraining(sc);
        double[] split = {0.5, 0.5};
        JavaRDD<UserItemEvent>[] trainingAndTestingViews = all.getViewEvents().randomSplit(split, 1);
        JavaRDD<UserItemEvent>[] trainingAndTestingBuys = all.getBuyEvents().randomSplit(split, 1);

        RDD<Tuple2<Query, Set<String>>> queryActual = JavaPairRDD.toRDD(trainingAndTestingViews[1].union(trainingAndTestingBuys[1]).groupBy(new Function<UserItemEvent, String>() {
            @Override
            public String call(UserItemEvent event) throws Exception {
                return event.getUser();
            }
        }).mapToPair(new PairFunction<Tuple2<String, Iterable<UserItemEvent>>, Query, Set<String>>() {
            @Override
            public Tuple2<Query, Set<String>> call(Tuple2<String, Iterable<UserItemEvent>> userEvents) throws Exception {
                Query query = new Query(userEvents._1(), 10, Collections.<String>emptySet(), Collections.<String>emptySet(), Collections.<String>emptySet());
                Set<String> actualSet = new HashSet<>();
                for (UserItemEvent event : userEvents._2()) {
                    actualSet.add(event.getItem());
                }
                return new Tuple2<>(query, actualSet);
            }
        }));

        Tuple3<TrainingData, EmptyParams, RDD<Tuple2<Query, Set<String>>>> setData = new Tuple3<>(new TrainingData(all.getUsers(), all.getItems(), trainingAndTestingViews[0], trainingAndTestingBuys[0]), new EmptyParams(), queryActual);

        return JavaConversions.asScalaIterable(Collections.singletonList(setData)).toSeq();
    }
}
