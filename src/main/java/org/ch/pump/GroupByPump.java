package org.ch.pump;

import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.tuple.Fields;

public class GroupByPump extends Pump {
  private final Pump prev;
  private final String[] fields;
  private String[] sortFields = new String[0];
  private boolean order;

  public GroupByPump(Pump prev, String[] fields) {
    this.order = false;
    this.prev = prev;
    this.fields = fields;
  }

  /**
   * Perform a secondary sort of these grouped tuples while grouping!
   * @param sortFields
   * @return
   */
  public GroupByPump secondarySort(String... sortFields) {
    this.sortFields = sortFields;
    return this;
  }

  /**
   * Reverse the ordering of the tuples when secondary sorting.
   * @return
   */
  public GroupByPump inReverse() {
    order = true;
    return this;
  }

  public Fields getFields() {
    return getArgSelector(fields);
  }

  @Override Pump getPrev() {
    return this.prev;
  }

  @Override public Pipe getPipeInternal() {
    return new GroupBy(prev.toPipe(), getArgSelector(fields), new Fields(sortFields), order);
  }
}
