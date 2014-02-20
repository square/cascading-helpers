package com.squareup.cascading_helpers.filter;

import cascading.flow.FlowProcess;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import com.squareup.cascading_helpers.operation.WrapperOperation;

public class StacktraceWrapperFilter extends WrapperOperation<Filter> implements Filter {
  private final String instantiationStackTrace;

  public StacktraceWrapperFilter(Filter wrappedFilter, String instantiationStackTrace) {
    super(wrappedFilter);
    this.instantiationStackTrace = instantiationStackTrace;
  }

  @Override public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    try {
      return wrappedOperation.isRemove(flowProcess, filterCall);
    } catch (Exception e) {
      throw new RuntimeException("Exception in operation instantiated at:\n" + instantiationStackTrace + "\nActual exception trace:", e);
    }
  }
}
