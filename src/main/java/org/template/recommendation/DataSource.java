package org.template.recommendation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prediction.controller.EmptyParams;
import io.prediction.controller.PDataSource;
import io.prediction.data.storage.Event;
import io.prediction.data.storage.PropertyMap;
import io.prediction.data.store.PEventStore;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.joda.time.DateTime;
import scala.Option;
import scala.Tuple2;
import scala.collection.JavaConversions$;
import scala.collection.Seq;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DataSource extends PDataSource<TrainingData, EmptyParams, Query, Object> {

    private final DataSourceParams dsp;

    public DataSource(DataSourceParams dsp) {
        this.dsp = dsp;
    }

    @Override
    public TrainingData readTraining(SparkContext sc) {
        JavaPairRDD<String,User> usersRDD = PEventStore.aggregateProperties(
                dsp.getAppName(),
                "user",
                Option.apply((String) null),
                Option.apply((DateTime) null),
                Option.apply((DateTime) null),
                Option.apply((Seq<String>) null),
                sc).toJavaRDD()
                .mapToPair(new PairFunction<Tuple2<String, PropertyMap>, String, User>() {
                    @Override
                    public Tuple2<String, User> call(Tuple2<String, PropertyMap> entityIdProperty) throws Exception {
                        Set<String> keys = JavaConversions$.MODULE$.setAsJavaSet(entityIdProperty._2().keySet());
                        Map<String, String> properties = new HashMap<>();
                        for (String key : keys) {
                            properties.put(key, Helper.dataMapGet(entityIdProperty._2(), key, String.class));
                        }

                        User user = new User(entityIdProperty._1(), ImmutableMap.copyOf(properties));

                        return new Tuple2<>(user.getEntityId(), user);
                    }
                });

        JavaPairRDD<String, Item> itemsRDD = PEventStore.aggregateProperties(
                dsp.getAppName(),
                "item",
                Option.apply((String) null),
                Option.apply((DateTime) null),
                Option.apply((DateTime) null),
                Option.apply((Seq<String>) null),
                sc).toJavaRDD()
                .mapToPair(new PairFunction<Tuple2<String, PropertyMap>, String, Item>() {
                    @Override
                    public Tuple2<String, Item> call(Tuple2<String, PropertyMap> entityIdProperty) throws Exception {
                        List<String> categories = JavaConversions$.MODULE$.seqAsJavaList(Helper.dataMapGetStringList(entityIdProperty._2(), "categories"));
                        Item item = new Item(entityIdProperty._1(), ImmutableList.copyOf(categories));

                        return new Tuple2<>(item.getEntityId(), item);
                    }
                });

        JavaRDD<UserItemEvent> viewEventsRDD = PEventStore.find(
                dsp.getAppName(),
                Option.apply((String) null),
                Option.apply((DateTime) null),
                Option.apply((DateTime) null),
                Option.apply("user"),
                Option.apply((String) null),
                Option.apply(JavaConversions$.MODULE$.collectionAsScalaIterable(Collections.singleton("view")).toSeq()),
                Option.apply((Option<String>) null),
                Option.apply((Option<String>) null),
                sc).toJavaRDD()
                .map(new Function<Event, UserItemEvent>() {
                    @Override
                    public UserItemEvent call(Event event) throws Exception {
                        return new UserItemEvent(event.entityId(), event.targetEntityId().get(), event.eventTime().getMillis(), UserItemEventType.VIEW);
                    }
                });

        JavaRDD<UserItemEvent> buyEventsRDD = PEventStore.find(
                dsp.getAppName(),
                Option.apply((String) null),
                Option.apply((DateTime) null),
                Option.apply((DateTime) null),
                Option.apply("user"),
                Option.apply((String) null),
                Option.apply(JavaConversions$.MODULE$.collectionAsScalaIterable(Collections.singleton("buy")).toSeq()),
                Option.apply((Option<String>) null),
                Option.apply((Option<String>) null),
                sc).toJavaRDD()
                .map(new Function<Event, UserItemEvent>() {
                    @Override
                    public UserItemEvent call(Event event) throws Exception {
                        return new UserItemEvent(event.entityId(), event.targetEntityId().get(), event.eventTime().getMillis(), UserItemEventType.BUY);
                    }
                });

        return new TrainingData(usersRDD, itemsRDD, viewEventsRDD, buyEventsRDD);
    }
}
