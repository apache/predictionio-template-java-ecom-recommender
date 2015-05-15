package org.template.recommendation.evaluation;

import io.prediction.controller.EmptyParams;
import io.prediction.controller.Metric;
import io.prediction.controller.java.SerializableComparator;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.rdd.RDD;
import org.template.recommendation.ItemScore;
import org.template.recommendation.PredictedResult;
import org.template.recommendation.Query;
import scala.Tuple2;
import scala.Tuple3;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class PrecisionMetric extends Metric<EmptyParams, Query, PredictedResult, Set<String>, Double> {

    private static final class MetricComparator implements SerializableComparator<Double> {
        @Override
        public int compare(Double o1, Double o2) {
            return o1.compareTo(o2);
        }
    }

    public PrecisionMetric() {
        super(new MetricComparator());
    }

    @Override
    public Double calculate(SparkContext sc, Seq<Tuple2<EmptyParams, RDD<Tuple3<Query, PredictedResult, Set<String>>>>> qpas) {
        List<Tuple2<EmptyParams, RDD<Tuple3<Query, PredictedResult, Set<String>>>>> sets = JavaConversions.asJavaList(qpas);
        List<Double> allSetResults = new ArrayList<>();

        for (Tuple2<EmptyParams, RDD<Tuple3<Query, PredictedResult, Set<String>>>> set : sets) {
            List<Double> setResults = set._2().toJavaRDD().map(new Function<Tuple3<Query, PredictedResult, Set<String>>, Double>() {
                @Override
                public Double call(Tuple3<Query, PredictedResult, Set<String>> qpa) throws Exception {
                    Set<String> predicted = new HashSet<>();
                    for (ItemScore itemScore : qpa._2().getItemScores()) {
                        predicted.add(itemScore.getItemEntityId());
                    }
                    Set<String> intersection = new HashSet<>(predicted);
                    intersection.retainAll(qpa._3());

                    return 1.0 * intersection.size() / qpa._2().getItemScores().size();
                }
            }).collect();

            allSetResults.addAll(setResults);
        }
        double sum = 0.0;
        for (Double value : allSetResults) sum += value;

        return sum / allSetResults.size();
    }
}
