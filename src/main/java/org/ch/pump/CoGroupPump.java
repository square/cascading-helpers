package org.ch.pump;

import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.Joiner;

public class CoGroupPump extends Pump {
  private final Pump left;
  private final String[] cogroupFields;
  private final Pump right;
  private final String[] modifiedCogroupFields;
  private final Joiner joiner;

  CoGroupPump(Pump left, String[] cogroupFields, Pump right, String[] modifiedCogroupFields,
      Joiner joiner) {
    super();
    this.left = left;
    this.cogroupFields = cogroupFields;
    this.right = right;
    this.modifiedCogroupFields = modifiedCogroupFields;
    this.joiner = joiner;
  }

  @Override Pump getPrev() {
    throw new UnsupportedOperationException("doesn't make sense to get the singular prev of a cogroup");
  }

  @Override public Pipe getPipeInternal() {
    return new CoGroup(left.toPipe(), getArgSelector(cogroupFields), right.toPipe(), getArgSelector(modifiedCogroupFields), joiner);
  }
}
