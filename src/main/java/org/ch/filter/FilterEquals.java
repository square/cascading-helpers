package org.ch.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Tuple;

/**
 * Remove tuples that match the exemplar tuple.
 */
public class FilterEquals extends BaseOperation implements Filter {
  private final Tuple exemplar;

  public FilterEquals(Object... exemplar) {
    this.exemplar = new Tuple(exemplar);
  }

  @Override public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    return filterCall.getArguments().getTuple().equals(exemplar);
  }
}
