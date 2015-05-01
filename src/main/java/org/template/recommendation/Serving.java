package org.template.recommendation;

import io.prediction.controller.LServing;
import scala.collection.Seq;

public class Serving extends LServing<Query, PredictedResult> {

    @Override
    public PredictedResult serve(Query query, Seq<PredictedResult> predictions) {
        return predictions.head();
    }
}
