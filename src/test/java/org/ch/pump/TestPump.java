package org.ch.pump;

import cascading.flow.hadoop.HadoopFlowConnector;
import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.operation.Debug;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexFilter;
import cascading.operation.text.DateFormatter;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Coerce;
import cascading.scheme.hadoop.TextLine;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.TupleEntryIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/** Author: duxbury */
public class TestPump extends TestCase {

  private static final String INPUT_PATH = "/tmp/TestPump/input";
  private static final String OUTPUT_PATH = "/tmp/TestPump/output";

  private static final List<Tuple> INPUT_TUPLES = new ArrayList<Tuple>(){{
    add(new Tuple("0"));
    add(new Tuple("115200000"));
    add(new Tuple("115200000"));
    add(new Tuple("asdf"));
  }};

  public void setUp() throws Exception {
    FileSystem.get(new Configuration()).delete(new Path(INPUT_PATH), true);
    FileSystem.get(new Configuration()).delete(new Path(OUTPUT_PATH), true);

    Tap inTap = getInTap();
    TupleEntryCollector tec = inTap.openForWrite(new HadoopFlowProcess());
    for (Tuple t : INPUT_TUPLES) {
      tec.add(new TupleEntry(t));
    }
    tec.close();
  }

  public Tap getInTap() {
    return new Hfs(new TextLine(), INPUT_PATH);
  }

  public Tap getOutTap() {
    return new Hfs(new TextLine(), OUTPUT_PATH);
  }

  public void testRetain() throws IOException {
    Pipe p = Pump.prime()
        .retain("line")
        .toPipe();

    new HadoopFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("0", "115200000", "115200000", "asdf"), getOutputStrings());
  }

  public void testDiscard() throws IOException {
    Pipe p = Pump.prime()
        .discard("offset")
        .toPipe();

    new HadoopFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("0", "115200000", "115200000", "asdf"), getOutputStrings());
  }

  // this is a pretty weak test, since the results are going to get stringified anyays
  public void testCoerce() throws Exception {
    Pipe p = Pump.prime()
        .discard("offset")
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .coerce("line", long.class)
        .toPipe();

    new HadoopFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("0", "115200000", "115200000"), getOutputStrings());
  }

  public void testEachFilter() throws IOException {
    Pipe p = Pump.prime()
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .retain("line")
        .toPipe();

    new HadoopFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("0", "115200000", "115200000"), getOutputStrings());
  }

  public void testEachFunction() throws IOException {
    Pipe p = Pump.prime()
        .each(new RegexFilter("^[0-9]+$", false), "line")
        .retain("line")
        .coerce("line", int.class)
        .each(new DateFormatter(new Fields("date"), "yyyy-MM-dd"))
        .retain("date")
        .toPipe();

    new HadoopFlowConnector().connect(getInTap(), getOutTap(), p).complete();

    assertEquals(Arrays.asList("1970-01-01", "1970-01-02", "1970-01-02"), getOutputStrings());
  }
  //
  //public void testSimple() throws IOException {
  //  fail();
  //  Pipe p = Pump.prime()
  //      .each(new RegexFilter("\\d+"))
  //      .each(new DateFormatter(new Fields("date"), "yyyy-MM-dd"))
  //      .groupby("date")
  //      .every(new Count(new Fields("count")))
  //      .toPipe();
  //
  //  new HadoopFlowConnector().connect(getInTap(), getOutTap(), p).complete();
  //
  //  Set<String> tuples = new HashSet<String>(getOutputStrings());
  //  assertEquals(new HashSet(Arrays.asList("1970-01-01\t1", "1970-01-02\t2")), getOutputStrings());
  //}

  private List<String> getOutputStrings() throws IOException {
    TupleEntryIterator iter = getOutTap().openForRead(new HadoopFlowProcess(), null);
    List<String> results = new ArrayList<String>();
    while (iter.hasNext()) {
      results.add(iter.next().getString(1));
    }
    return results;
  }
}
