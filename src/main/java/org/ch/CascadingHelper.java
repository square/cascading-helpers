package org.ch;

import cascading.flow.FlowConnector;
import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.tuple.hadoop.TupleSerialization;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.serializer.Serialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;

/**
 * CascadingHelper is designed to help you avoid the pain of using a standardized (and
 * environment-aware) configuration across your codebase. Out of the box, the only concrete benefit
 * that it offers is the use of setTestMode, which will make your unit tests run much, much faster.
 * However, you are encouraged to extend CascadingHelper and modify the default properties, add
 * serializations, and add classes that need serialization tokens.
 */
public class CascadingHelper {
  private static final String IO_SERIALIZATIONS = "io.serializations";
  private static boolean testMode = false;

  protected static final Map<Object, Object> DEFAULT_PROPERTIES = new HashMap<Object, Object>();
  protected static final List<Class<? extends Serialization>> SERIALIZATION_IMPLS =
      new ArrayList<Class<? extends Serialization>>(Arrays.asList(
          WritableSerialization.class,
          TupleSerialization.class
      ));

  private static final int STARTING_TOKEN = 128;
  protected static final List<Class> CLASSES_TO_BE_SERIALIZED = new ArrayList<Class>();

  private static final CascadingHelper THE_HELPER = new CascadingHelper();

  public static CascadingHelper get() {
    return THE_HELPER;
  }

  /**
   * After calling this method, Cascading Flows will exit much more quickly, which is crucial when
   * running test flows with very little data.
   */
  public static void setTestMode() {
    testMode = true;
  }

  private static Map<Object, Object> mergeProperties(Map<Object, Object> properties) {
    Map<Object, Object> result = new HashMap<Object, Object>();
    result.putAll(DEFAULT_PROPERTIES);
    addSerializations(result);
    assignSerializationTokens(result);
    if (testMode) {
      // this causes flows to complete more quickly in test mode, at the expense of a bit of CPU thrashing.
      result.put("cascading.flow.job.pollinginterval", 10);
    }
    result.putAll(properties);
    return result;
  }

  private static void assignSerializationTokens(Map<Object, Object> props) {
    StringBuilder sb = new StringBuilder("");
    int token = STARTING_TOKEN;
    for (Class klass : CLASSES_TO_BE_SERIALIZED) {
      if (token != STARTING_TOKEN) {
        sb.append(",");
      }
      sb.append(token++).append("=").append(klass.getName());
    }
    props.put("cascading.serialization.tokens", sb.toString());
  }

  private static void addSerializations(Map<Object, Object> props) {
    JobConf jobConf = new JobConf();
    String existingSerializations = jobConf.get(IO_SERIALIZATIONS);
    ArrayList<String> serializations =
        new ArrayList(Arrays.asList(existingSerializations.split(",")));
    for (Class<? extends Serialization> serClass : SERIALIZATION_IMPLS) {
      serializations.add(serClass.getName());
    }
    StringBuilder sb = new StringBuilder("");
    boolean first = true;
    for (String serialization : serializations) {
      if (!first) {
        sb.append(",");
      }
      sb.append(serialization);
      first = false;
    }
    props.put(IO_SERIALIZATIONS, sb.toString());
  }

  // private so that this class may not be instantiated
  protected CascadingHelper() {}

  public FlowConnector getFlowConnector() {
    return getFlowConnector(Collections.emptyMap());
  }

  public FlowConnector getFlowConnector(Map<Object, Object> properties) {
    return new HadoopFlowConnector(mergeProperties(properties));
  }
}
