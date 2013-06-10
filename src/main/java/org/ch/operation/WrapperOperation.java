package org.ch.operation;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Operation;
import cascading.operation.OperationCall;

public abstract class WrapperOperation<T extends Operation> extends BaseOperation {
  protected final T wrappedOperation;

  public WrapperOperation(T wrappedOperation) {
    super(wrappedOperation.getNumArgs(), wrappedOperation.getFieldDeclaration());
    this.wrappedOperation = wrappedOperation;
  }

  @Override public void prepare(FlowProcess flowProcess, OperationCall operationCall) {
    wrappedOperation.prepare(flowProcess, operationCall);
  }

  @Override public void cleanup(FlowProcess flowProcess, OperationCall operationCall) {
    wrappedOperation.cleanup(flowProcess, operationCall);
  }

  @Override public void flush(FlowProcess flowProcess, OperationCall operationCall) {
    wrappedOperation.flush(flowProcess, operationCall);
  }
}
