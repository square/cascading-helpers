package com.squareup.cascading_helpers.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import java.util.Map;

/** Author: duxbury */
public class MapLookup extends BaseOperation implements Function {
  private Map<Tuple, Tuple> map;

  public MapLookup(Map<Tuple, Tuple> map, Fields fieldDeclaration) {
    super(fieldDeclaration);
    this.map = map;
  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    Tuple result = map.get(functionCall.getArguments().getTuple());
    if (result == null) {
      result = new Tuple(new Object[getFieldDeclaration().size()]);
    }
    functionCall.getOutputCollector().add(result);
  }
}
