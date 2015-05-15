package org.template.recommendation.evaluation;

import io.prediction.controller.Engine;
import io.prediction.controller.java.JavaEvaluation;
import io.prediction.core.BaseAlgorithm;
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
