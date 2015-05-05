package org.template.recommendation;

import io.prediction.controller.Params;

import java.util.List;

public class AlgorithmParams implements Params{
    private final long seed;
    private final int rank;
    private final int iteration;
    private final double lambda;
    private final String appName;
    private final List<String> similarItemEvents;
    private final boolean unseenOnly;
    private final List<String> seenItemEvents;


    public AlgorithmParams(long seed, int rank, int iteration, double lambda, String appName, List<String> similarItemEvents, boolean unseenOnly, List<String> seenItemEvents) {
        this.seed = seed;
        this.rank = rank;
        this.iteration = iteration;
        this.lambda = lambda;
        this.appName = appName;
        this.similarItemEvents = similarItemEvents;
        this.unseenOnly = unseenOnly;
        this.seenItemEvents = seenItemEvents;
    }

    public long getSeed() {
        return seed;
    }

    public int getRank() {
        return rank;
    }

    public int getIteration() {
        return iteration;
    }

    public double getLambda() {
        return lambda;
    }

    public String getAppName() {
        return appName;
    }

    public List<String> getSimilarItemEvents() {
        return similarItemEvents;
    }

    public boolean isUnseenOnly() {
        return unseenOnly;
    }

    public List<String> getSeenItemEvents() {
        return seenItemEvents;
    }

    @Override
    public String toString() {
        return "AlgorithmParams{" +
                "seed=" + seed +
                ", rank=" + rank +
                ", iteration=" + iteration +
                ", lambda=" + lambda +
                ", appName='" + appName + '\'' +
                ", similarItemEvents=" + similarItemEvents +
                ", unseenOnly=" + unseenOnly +
                ", seenItemEvents=" + seenItemEvents +
                '}';
    }
}
