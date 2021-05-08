package com.splicemachine.derby.impl.sql.execute.actions;

import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

public class DependencyBucketingTest{
    DependencyBucketing<Character> cut = new DependencyBucketing<>();

    private void assertBuckets(String expected) {
        StringBuilder result = new StringBuilder().append("[");
        for (int i = 0; i < cut.getBuckets().size(); ++i) {
            List<Character> bucket = cut.getBuckets().get(i);
            result.append("[");
            result.append(bucket.stream().map(String::valueOf).sorted().collect(Collectors.joining(", ")));
            result.append("]");
            if (i < cut.getBuckets().size() - 1) {
               result.append(", ");
            }
        }
        result.append("]");
        assertEquals(expected, result.toString());
    }

    @Test
    public void testEmpty() {
        assertBuckets("[[], []]");
    }

    @Test
    public void testSingleNode() {
        cut.addSingleNode('A');
        assertBuckets("[[A], []]");
    }

    @Test
    public void testOneDependency() {
        cut.addDependency('A', 'B');
        assertBuckets("[[A], [B]]");
    }

    @Test
    public void testAddSingleNodeAfterDependency() {
        cut.addDependency('A', 'B');
        cut.addSingleNode('B');
        assertBuckets("[[A], [B]]");
    }

    @Test
    public void testUnrelatedDependencies() {
        cut.addDependency('A', 'B');
        cut.addDependency('C', 'D');
        assertBuckets("[[A, C], [B, D]]");
    }

    @Test
    public void testSimpleChain() {
        cut.addDependency('A', 'B');
        cut.addDependency('B', 'C');
        assertBuckets("[[A], [B], [C]]");
    }

    @Test
    public void testMultipleChildren() {
        cut.addDependency('A', 'B');
        cut.addDependency('A', 'C');
        assertBuckets("[[A], [B, C]]");
    }

    @Test
    public void testMultipleParents() {
        cut.addDependency('A', 'C');
        cut.addDependency('B', 'C');
        assertBuckets("[[A, B], [C]]");
    }

    @Test
    public void testComplexScenario() {
        cut.addDependency('B', 'A');
        cut.addDependency('C', 'A');
        cut.addDependency('E', 'B');
        cut.addDependency('D', 'C');
        cut.addDependency('F', 'C');
        cut.addDependency('E', 'D');
        assertBuckets("[[E, F], [B, D], [C], [A]]");
    }
}