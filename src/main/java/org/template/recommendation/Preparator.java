package org.template.recommendation;

import io.prediction.controller.java.PJavaPreparator;
import org.apache.spark.SparkContext;

public class Preparator extends PJavaPreparator<TrainingData, PreparedData> {

    @Override
    public PreparedData prepare(SparkContext sc, TrainingData trainingData) {
        return new PreparedData(trainingData);
    }
}
