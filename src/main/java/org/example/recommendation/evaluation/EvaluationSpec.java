package org.example.recommendation.evaluation;

import org.apache.predictionio.controller.Engine;
import org.apache.predictionio.controller.java.JavaEvaluation;
import org.apache.predictionio.core.BaseAlgorithm;
import org.example.recommendation.Algorithm;
import org.example.recommendation.DataSource;
import org.example.recommendation.PredictedResult;
import org.example.recommendation.Preparator;
import org.example.recommendation.PreparedData;
import org.example.recommendation.Query;
import org.example.recommendation.Serving;

import java.util.Collections;

public class EvaluationSpec extends JavaEvaluation {
    public EvaluationSpec() {
        this.setEngineMetric(
                new Engine<>(
                        DataSource.class,
                        Preparator.class,
                        Collections.<String, Class<? extends BaseAlgorithm<PreparedData, ?, Query, PredictedResult>>>singletonMap("algo", Algorithm.class),
                        Serving.class
                ),
                new PrecisionMetric()
        );
    }
}
