package com.squareup.cascading_helpers.pump;

public abstract class InternalPump extends Pump {
  private final Pump prev;

  private final String stackTrace;

  protected InternalPump(Pump prev) {
    this.prev = prev;
    stackTrace = captureStackTrace();
  }

  private String captureStackTrace() {
    StackTraceElement[] stackTrace = Thread.currentThread().getStackTrace();

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (int i = 5; i < stackTrace.length; i++) {
      StackTraceElement element = stackTrace[i];

      if (!first) {
        sb.append("\n");
      }
      first = false;
      sb.append("\tat ")
          .append(element.getClassName())
          .append(".")
          .append(element.getMethodName())
          .append("(")
          .append(element.getFileName())
          .append(":")
          .append(element.getLineNumber())
          .append(")");
    }
    return sb.toString();
  }

  @Override Pump getPrev() {
    return prev;
  }

  public String getStackTrace() {
    return stackTrace;
  }
}
