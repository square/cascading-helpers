package org.ch.pump;

public abstract class InternalPump extends Pump {
  private final Pump prev;

  protected InternalPump(Pump prev) {
    this.prev = prev;
  }

  @Override Pump getPrev() {
    return prev;
  }
}
