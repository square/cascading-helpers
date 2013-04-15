package org.ch.pump;

import cascading.operation.Aggregator;
import cascading.pipe.Every;
import cascading.pipe.Pipe;

public class EveryPump extends InternalPump {
  private final Aggregator agg;
  private final String[] args;

  public EveryPump(Pump prev, Aggregator agg, String[] args) {
    super(prev);
    this.agg = agg;
    this.args = args;
  }

  @Override Pipe toPipe() {
    return new Every(getPrev().toPipe(), getArgSelector(args), agg);
  }
}
