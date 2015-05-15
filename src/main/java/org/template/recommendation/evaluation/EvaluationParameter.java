package org.template.recommendation.evaluation;

import io.prediction.controller.EmptyParams;
import io.prediction.controller.EngineParams;
import io.prediction.controller.java.JavaEngineParamsGenerator;
import org.template.recommendation.AlgorithmParams;
import org.template.recommendation.DataSourceParams;

import java.util.Arrays;
import java.util.Collections;

public class EvaluationParameter extends JavaEngineParamsGenerator {
    public EvaluationParameter() {
        this.setEngineParamsList(
                Collections.singletonList(
                        new EngineParams(
                                "",
                                new DataSourceParams("javadase"),
                                "",
                                new EmptyParams(),
                                Collections.singletonMap("algo", new AlgorithmParams(1, 10, 10, 0.01, "javadase", Collections.singletonList("view"), true, Arrays.asList("buy", "view"))),
                                "",
                                new EmptyParams()
                        )
                )
        );
    }
}
