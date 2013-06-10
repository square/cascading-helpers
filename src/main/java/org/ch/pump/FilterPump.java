package org.ch.pump;

import cascading.operation.Filter;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import org.ch.filter.StacktraceWrapperFilter;

public class FilterPump extends InternalPump {
  private final Filter filter;
  private final String[] args;

  public FilterPump(Pump prev, Filter filter, String[] args) {
    super(prev);
    this.filter = filter;
    this.args = args;
  }

  @Override public Pipe toPipe() {
    return new Each(getPrev().toPipe(), getArgSelector(args), new StacktraceWrapperFilter(filter, getStackTrace()));
  }
}
