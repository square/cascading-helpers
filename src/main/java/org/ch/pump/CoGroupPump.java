package org.ch.pump;

import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.joiner.Joiner;
import java.util.HashSet;
import java.util.Set;

public class CoGroupPump extends Pump {
  private final Pump left;
  private final String[] cogroupFields;
  private final Pump right;
  private final String[] modifiedCogroupFields;
  private final Joiner joiner;

  @Override public Set<Class> getEmittedClasses() {
    Set<Class> leftClasses = left.getEmittedClasses();
    Set<Class> rightClasses = right.getEmittedClasses();
    Set<Class> combined = new HashSet<Class>();
    combined.addAll(leftClasses);
    combined.addAll(rightClasses);
    return combined;
  }

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
