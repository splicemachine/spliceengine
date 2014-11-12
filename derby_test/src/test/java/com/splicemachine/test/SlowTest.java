package com.splicemachine.test;

/**
 * Used together with JUnit's @Category to annotate tests which should not run by default on a developer's machine.
 * Currently slow tests do run on Jenkins.
 */
public interface SlowTest {
}
