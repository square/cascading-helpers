package org.ch.pump;

import cascading.operation.Aggregator;
import cascading.pipe.Every;
import cascading.pipe.Pipe;

public class AggregatorPump extends EveryPump {
  private final Aggregator agg;

  public AggregatorPump(Pump prev, Aggregator agg, String[] args) {
    super(prev, args);
    this.agg = agg;
  }

  @Override Pipe toPipe() {
    return new Every(getPrev().toPipe(), getArgSelector(args), agg);
  }
}
