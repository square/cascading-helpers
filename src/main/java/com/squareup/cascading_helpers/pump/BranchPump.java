package com.squareup.cascading_helpers.pump;

import cascading.pipe.Pipe;

public class BranchPump extends InternalPump {
  private final String branchName;

  public BranchPump(Pump prev, String branchName) {
    super(prev);
    this.branchName = branchName;
  }

  @Override Pipe getPipeInternal() {
    return new Pipe(branchName, getPrev().toPipe());
  }
}
