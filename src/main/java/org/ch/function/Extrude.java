package org.ch.function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Extrude emits one Tuple per argument field, with the argument field copied into the output field.
 *
 * For instance, with the input Tuple [a, b, c] and the argument fields [b, c], after Extrude, you
 * will have Tuples [a, b] and [a, c].
 */
public class Extrude extends BaseOperation implements Function {
  public Extrude(String outputField) {
    super(new Fields(outputField));
  }

  @Override public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
    TupleEntry arguments = functionCall.getArguments();
    TupleEntryCollector outputCollector = functionCall.getOutputCollector();
    for (int argNum = 0; argNum < arguments.size(); argNum++) {
      outputCollector.add(new Tuple(arguments.getObject(argNum)));
    }
  }
}
