package com.splicemachine.management.client;

import com.google.gwt.core.client.EntryPoint;

/**
 * Entry point classes define <code>onModuleLoad()</code>.
 */
public class splice implements EntryPoint {
  /**
   * This is the entry point method.
   */
  private SpliceLayout content;

  public void onModuleLoad() {
			content = new SpliceLayout();
			content.draw();
  }
}