package org.template.recommendation;

import io.prediction.controller.EmptyParams;
import io.prediction.controller.Engine;
import io.prediction.controller.EngineFactory;
import io.prediction.core.BaseAlgorithm;
import io.prediction.core.BaseEngine;

import java.util.Collections;
import java.util.Set;

public class RecommendationEngine extends EngineFactory {

    @Override
    public BaseEngine<EmptyParams, Query, PredictedResult, Set<String>> apply() {
        return new Engine<>(
                DataSource.class,
                Preparator.class,
                Collections.<String, Class<? extends BaseAlgorithm<PreparedData, ?, Query, PredictedResult>>>singletonMap("algo", Algorithm.class),
                Serving.class
        );
    }
}
