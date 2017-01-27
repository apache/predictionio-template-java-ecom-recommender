package org.template.recommendation.evaluation;

import org.apache.predictionio.controller.Engine;
import org.apache.predictionio.controller.java.JavaEvaluation;
import org.apache.predictionio.core.BaseAlgorithm;
import org.template.recommendation.Algorithm;
import org.template.recommendation.DataSource;
import org.template.recommendation.PredictedResult;
import org.template.recommendation.Preparator;
import org.template.recommendation.PreparedData;
import org.template.recommendation.Query;
import org.template.recommendation.Serving;

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
