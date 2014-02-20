package com.squareup.cascading_helpers.operation;

import cascading.operation.Function;
import java.util.Set;

/**
 * Does your Function know which classes it can emit? Tell us so we can analyze!
 */
public interface KnowsEmittedClasses extends Function {
  public Set<Class> getEmittedClasses();
}
