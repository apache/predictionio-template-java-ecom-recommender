package org.template.recommendation;

import io.prediction.controller.*;
import io.prediction.core.BaseAlgorithm;
import io.prediction.core.BaseEngine;
import scala.collection.JavaConversions;

import java.util.HashMap;
import java.util.Map;

public class RecommendationEngine implements EngineFactory {

    @Override
    public BaseEngine<EmptyParams, Query, PredictedResult, Object> apply() {
        Map<String, Class<? extends BaseAlgorithm<PreparedData, ?, Query, PredictedResult>>> algorithmMap = new HashMap<>(1);
        algorithmMap.put("algo", Algorithm.class);

        scala.collection.mutable.Map<String, Class<? extends BaseAlgorithm<PreparedData, ?, Query, PredictedResult>>> scalaMap = JavaConversions.asScalaMap(algorithmMap);

        return new Engine<>(
                DataSource.class,
                Preparator.class,
                scalaMap.toMap(scala.Predef$.MODULE$.<scala.Tuple2<String, Class<? extends BaseAlgorithm<PreparedData, ?, Query, PredictedResult>> >>conforms()),
                Serving.class
        );
    }

    @Override
    public EngineParams engineParams(String key) {
        return EngineFactory$class.engineParams(this, key);
    }
}
