package com.squareup.cascading_helpers.filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.Fields;
import java.util.Arrays;
import java.util.List;

/**
 * Fail the job if any of the specified fields are null in the stream
 */
public class FailOnNull extends BaseOperation implements Filter {
  private final String errorText;
  private final List<String> fields;

  public FailOnNull(String errorText, String[] args) {
    if (errorText == null) {
      errorText = "Expected no null tuples, but found one!";
    }
    this.fields = Arrays.asList(args);
    this.errorText = errorText;
  }

  @Override public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
    for (String field : this.fields) {
      if (filterCall.getArguments().getObject(field) == null)
        throw new NullPointerException(errorText);
    }
    return false;
  }
}
