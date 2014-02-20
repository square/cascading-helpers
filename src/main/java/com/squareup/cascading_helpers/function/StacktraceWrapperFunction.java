package com.squareup.cascading_helpers.function;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import com.squareup.cascading_helpers.operation.WrapperOperation;

public class StacktraceWrapperFunction extends WrapperOperation<Function> implements Function {
  private final String instantiationStackTrace;

  public StacktraceWrapperFunction(Function wrappedFunction, String instantiationStackTrace) {
    super(wrappedFunction);
    this.instantiationStackTrace = instantiationStackTrace;
  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    try {
      wrappedOperation.operate(flowProcess, functionCall);
    } catch (Exception e) {
      throw new RuntimeException("Exception in operation instantiated at:\n" + instantiationStackTrace + "\nActual exception trace:", e);
    }
  }
}
