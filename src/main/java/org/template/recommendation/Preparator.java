package org.template.recommendation;

import io.prediction.controller.PPreparator;
import org.apache.spark.SparkContext;

public class Preparator extends PPreparator<TrainingData, PreparedData> {

    @Override
    public PreparedData prepare(SparkContext sc, TrainingData trainingData) {
        return new PreparedData(trainingData);
    }
}
