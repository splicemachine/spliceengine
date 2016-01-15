package com.splicemachine.test;

/**
 * Used together with JUnit's @Category to annotate one of a very small number
 * of integration tests (ITs) to be run AFTER all other ITs complete,
 * in order to assess the state of the system after a significant amount
 * of activity. For example, we can use this mechanism to assert that
 * proper cleanup has occurred across the board.
 */
public interface FinalTest {
}
