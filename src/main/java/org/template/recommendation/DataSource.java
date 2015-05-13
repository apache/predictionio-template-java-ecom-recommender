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
import org.joda.time.DateTime;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions$;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataSource extends PJavaDataSource<TrainingData, EmptyParams, Query, Object> {

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
}
