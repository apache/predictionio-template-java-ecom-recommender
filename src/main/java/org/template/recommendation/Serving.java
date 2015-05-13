package org.template.recommendation;

import io.prediction.controller.java.LJavaServing;
import scala.collection.Seq;

public class Serving extends LJavaServing<Query, PredictedResult> {

    @Override
    public PredictedResult serve(Query query, Seq<PredictedResult> predictions) {
        return predictions.head();
    }
}
