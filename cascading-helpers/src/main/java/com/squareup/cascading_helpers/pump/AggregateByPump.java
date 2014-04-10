package com.squareup.cascading_helpers.pump;

import cascading.operation.Aggregator;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.AggregateBy;
import java.util.ArrayList;
import java.util.List;

public class AggregateByPump extends InternalPump {
  private final AggregateBy aggregateBy;

  public AggregateByPump(Pump prev, AggregateBy aggregateBy) {
    super(prev);
    this.aggregateBy = aggregateBy;
  }

  public AggregateByPump(Pump prev, AggregateBy.Functor functor, Aggregator aggregator,
      String[] argumentFields) {
    super(prev);
    this.aggregateBy = new InternalAggregateBy(functor, aggregator, argumentFields);
  }

  @Override Pipe getPipeInternal() {
    Pump cur = this;
    List<AggregateBy> aggregators = new ArrayList<AggregateBy>();
    while (true) {
      if (cur instanceof AggregateByPump) {
        aggregators.add(((AggregateByPump)cur).aggregateBy);
        cur = cur.getPrev();
      } else if (cur instanceof GroupByPump) {
        GroupByPump groupby = (GroupByPump)cur;
        if (groupby.hasSortFields()) {
          throw new IllegalArgumentException(
              "Partial aggregator does not support custom sort fields. "
                  + "Argument fields are automatically used for secondary sorting.");
        }
        return new AggregateBy(cur.getPrev().toPipe(), ((GroupByPump)cur).getFields(),
            aggregators.toArray(new AggregateBy[aggregators.size()]));
      } else {
        throw new IllegalArgumentException(
            "Partial aggregator must follow group by or other partial aggregator");
      }
    }
  }

  private static class InternalAggregateBy extends AggregateBy {
    public InternalAggregateBy(Functor functor, Aggregator aggregator, String[] argumentFields) {
      super(getArgSelector(argumentFields), functor, aggregator);
    }
  }
}
