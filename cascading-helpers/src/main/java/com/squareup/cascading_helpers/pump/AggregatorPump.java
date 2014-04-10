package com.squareup.cascading_helpers.pump;

import cascading.operation.Aggregator;
import cascading.pipe.Every;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class AggregatorPump extends EveryPump {
  private final Aggregator agg;

  public AggregatorPump(Pump prev, Aggregator agg, String[] args) {
    super(prev, args);
    this.agg = agg;
  }

  @Override public Pipe getPipeInternal() {
    /*
     * Use Fields.VALUES as the default field.
     * This allows Pump#every and the aggregator to be called with no arguments while avoiding the
     * name conflict that would happen if the aggregator uses the input arguments to name the output
     * (as is done by First, for example).
     */
    return new Every(getPrev().toPipe(), getArgSelector(Fields.VALUES, args), agg);
  }
}
