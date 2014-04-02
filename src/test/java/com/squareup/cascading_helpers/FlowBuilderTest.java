package com.squareup.cascading_helpers;

import cascading.flow.Flow;
import cascading.flow.FlowListener;
import com.squareup.cascading_helpers.pump.Pump;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class FlowBuilderTest {
  @Before
  public void setUp() throws Exception {
    CascadingHelper.setTestMode();
    FileSystem.get(new Configuration()).delete(new Path(Tests.INPUT_PATH), true);
    FileSystem.get(new Configuration()).delete(new Path(Tests.TRAP_PATH), true);
    FileSystem.get(new Configuration()).delete(new Path(Tests.OUTPUT_PATH), true);
    Tests.fillTap(Tests.INPUT_TUPLES, Tests.getTap(Tests.INPUT_PATH));
  }

  @Test
  public void happyPath() throws Exception {
    Pump p = Pump.prime("input")
        .each(new Tests.FunctionThatKnows(Tests.Left.class))
        .retain("line");

    TestListener listener = new TestListener();
    String name = "testing FlowBuilder";
    HashMap<Object, Object> properties = new HashMap<Object, Object>();
    properties.put("some hadoop stuff", "as this thing");

    FlowBuilder builder = new FlowBuilder().name(name)
        .source(p, Tests.getInTap())
        .trap(p, Tests.getOutTap())
        .tailSink(p, Tests.getTrap())
        .listeners(Arrays.<FlowListener>asList(listener))
        .properties(properties);

    assertThat(builder.getEmittedClasses(), hasItem(Tests.Left.class));

    Flow flow = builder.build();
    flow.complete();

    assertNotNull(flow.getSource("input"));
    assertFalse(flow.getTraps().isEmpty());
    assertThat(flow.getName(), equalTo(name));
    assertFalse(flow.getSinks().isEmpty());

    assertTrue(listener.completed);
  }

  private static class TestListener implements FlowListener {
    public boolean completed = false;

    @Override public void onStarting(Flow flow) {
    }

    @Override public void onStopping(Flow flow) {
    }

    @Override public void onCompleted(Flow flow) {
      this.completed = true;
    }

    @Override public boolean onThrowable(Flow flow, Throwable throwable) {
      return false;
    }
  }
}
