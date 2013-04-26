package org.ch.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

public class GetOrElse extends BaseOperation implements Function {
  private final Tuple value;

  public GetOrElse(Tuple value, String name) {
    super(1, new Fields(name));
    if (value.size() != 1) {
      throw new IllegalArgumentException("tuple size must be 1");
    }
    this.value = value;
  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    Tuple value = functionCall.getArguments().getTuple();
    if (value.getObject(0) == null) {
      value = this.value;
    }
    functionCall.getOutputCollector().add(value);
  }
}