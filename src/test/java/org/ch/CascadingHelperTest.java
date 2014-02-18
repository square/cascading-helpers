package org.ch;

import org.junit.Test;

import java.util.Map;

import static junit.framework.Assert.assertTrue;

public class CascadingHelperTest {
  private static final class X {}
  private static final class Y {}
  private static final class Z {}

  @Test
  public void testWithTokensFor() throws Exception {
    Map<Object,Object> properties = CascadingHelper.get().withTokensFor(X.class, Y.class).getFlowConnector().getProperties();
    String tokensString = (String) properties.get("cascading.serialization.tokens");
    assertTrue(tokensString.contains(X.class.getName()));
    assertTrue(tokensString.contains(Y.class.getName()));

    properties = CascadingHelper.get().withTokensFor(X.class, Y.class, Z.class).getFlowConnector().getProperties();
    tokensString = (String) properties.get("cascading.serialization.tokens");
    assertTrue(tokensString.contains(X.class.getName()));
    assertTrue(tokensString.contains(Y.class.getName()));
    assertTrue(tokensString.contains(Z.class.getName()));
  }
}
