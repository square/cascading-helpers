package com.squareup.cascading_helpers.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;

/**
 * Fail the job if there are unexpected nulls in the stream
 */
public class FailOnNull extends BaseOperation implements Filter {
  private final String errorText;

  public FailOnNull(String errorText) {
    if (errorText == null) {
      errorText = "Expected no null tuples, but found one!";
    }
    this.errorText = errorText;
  }

  @Override public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    if (filterCall.getArguments().getObject(0) == null) {
      throw new RuntimeException(errorText);
    }
    return false;
  }
}
