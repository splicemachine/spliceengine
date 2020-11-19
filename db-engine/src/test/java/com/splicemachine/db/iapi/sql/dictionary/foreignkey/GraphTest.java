/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.iapi.sql.dictionary.foreignkey;

import com.splicemachine.db.iapi.error.StandardException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.util.*;
import java.util.stream.IntStream;

@RunWith(Enclosed.class)
public class GraphTest {

    interface PermuteClosure {
        void call(List<String> permutation) throws StandardException;
    }

    static void Permute(List<String> input, int k, PermuteClosure action) throws StandardException {
        if (k == 1) {
            action.call(input);
        }
        for (int i = 0; i < k; ++i) {
            Permute(input, k - 1, action);
            if (i < k - 1) {
                if (k % 2 == 0) {
                    Collections.swap(input, i, k - 1);
                } else {
                    Collections.swap(input, 0, input.size() - 1);
                }
            }
        }
    }

    static List<String> permute(String graphviz) throws StandardException {
        final List<String> edges = Lists.newArrayList(graphviz.split("\n"));
        final String header = edges.remove(0);
        final String footer = edges.remove(edges.size() - 1);
        final List<String> result = new ArrayList<>(IntStream.rangeClosed(1, edges.size()).reduce(1, (int x, int y) -> x * y));
        result.add(graphviz);
        Permute(edges, edges.size(), permutation -> {
            StringBuilder permutedGraph = new StringBuilder(header);
            for (String edge : permutation) {
                permutedGraph.append(edge);
            }
            permutedGraph.append(footer);
            result.add(permutedGraph.toString());
        });
        return result;
    }

    static class NegativeTestCase {
        final String graph;
        final String pathA;
        final String pathB;
        final String deleteActionA;
        final String deleteActionB;
        final String table;

        NegativeTestCase(String graph, String table, String pathA, String deleteActionA, String pathB, String deleteActionB) {
            this.graph = graph;
            this.pathA = pathA;
            this.pathB = pathB;
            this.deleteActionA = deleteActionA;
            this.deleteActionB = deleteActionB;
            this.table = table;
        }

        @Override
        public String toString() {
            return graph;
        }
    }

    @RunWith(Parameterized.class)
    public static class NegativeTests {

        private static Collection<Object[]> parameters = Lists.newArrayListWithCapacity(100);

        private static void addCase(String graph, String table, String pathA, String deleteActionA, String pathB, String deleteActionB) throws StandardException {
            parameters.add(new Object[]{new NegativeTestCase(graph, table, pathA, deleteActionA, pathB, deleteActionB)});
            List<String> invalidGraph1Permutations = permute(graph);
            for (String permutedGraph : invalidGraph1Permutations) {
                parameters.add(new Object[]{new NegativeTestCase(permutedGraph, table, pathA, deleteActionA, pathB, deleteActionB)});
            }
        }

        @Parameterized.Parameters(name = "negative case --- {0}")
        public static Collection<Object[]> data() throws StandardException {
            addCase("digraph {\n" +
                            "A -> B[label=\"Cascade\"];\n" +
                            "B -> C[label=\"Cascade\"];\n" +
                            "C -> A[label=\"SetNull\"];\n" +
                            "C -> A[label=\"Restrict\"];\n" +
                            "}", "C", "A", "SetNull", "A", "Restrict");
            addCase("digraph {\n" +
                            "T2 -> T1[label=\"Cascade\"];\n" +
                            "T3 -> T1[label=\"Cascade\"];\n" +
                            "T6 -> T2[label=\"Cascade\"];\n" +
                            "T7 -> T6[label=\"Cascade\"];\n" +
                            "T7 -> T4[label=\"SetNull\"];\n" +
                            "T4 -> T3[label=\"Cascade\"];\n" +
                            "}", "T7", "T1 T2 T6", "Cascade", "T1 T3 T4", "SetNull");
            addCase("digraph {\n" +
                            "B -> A[label=\"Cascade\"];\n" +
                            "C -> A[label=\"Cascade\"];\n" +
                            "D -> B[label=\"Cascade\"];\n" +
                            "D -> C[label=\"SetNull\"];\n" +
                            "}", "D", "A C", "SetNull", "A B", "Cascade");
            addCase("digraph {\n" +
                            "A -> B[label=\"Cascade\"];\n" +
                            "A -> B[label=\"SetNull\"];\n" +
                            "}", "A", "B", "SetNull", "B", "Cascade");
            addCase("digraph {\n" +
                            "B -> A[label=\"Cascade\"];\n" +
                            "C -> A[label=\"Cascade\"];\n" +
                            "D -> B[label=\"Cascade\"];\n" +
                            "D -> C[label=\"SetNull\"];\n" +
                            "}", "D", "A C", "SetNull", "A B", "Cascade");
            return parameters;
        }

        public NegativeTests(NegativeTestCase negativeTestCase) {
            this.negativeTestCase = negativeTestCase;
        }

        NegativeTestCase negativeTestCase;

        @Test
        public void testcase() throws Exception {
            Graph graph = new GraphvizParser
                    (negativeTestCase.graph).generateGraph("something");

            GraphAnnotator annotater = new GraphAnnotator("something", graph);
            annotater.annotate();
            try {
                annotater.analyzeAnnotations();
                Assert.fail("the annotator should have failed with error containing: adding this foreign key leads to the conflicting delete actions on the table ...");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof StandardException);
                StandardException sqlException = (StandardException) e;
                Assert.assertEquals("42915", sqlException.getSQLState());
                Assert.assertTrue(sqlException.getMessage().contains("adding this foreign key leads to the conflicting delete actions on the table '" + negativeTestCase.table + "'"));
                Assert.assertTrue(sqlException.getMessage().contains(negativeTestCase.pathA + " (delete action: " + negativeTestCase.deleteActionA + ")"));
                Assert.assertTrue(sqlException.getMessage().contains(negativeTestCase.pathB + " (delete action: " + negativeTestCase.deleteActionB + ")"));
            }
        }
    }

    @RunWith(Parameterized.class)
    public static class PositiveTests {

        private static Collection<Object[]> parameters = Lists.newArrayListWithCapacity(100);

        private static void addCase(String graph) throws StandardException {
            parameters.add(new Object[]{graph});
            List<String> validGraphPermutations = permute(graph);
            for (String permutedGraph : validGraphPermutations) {
                parameters.add(new Object[]{permutedGraph});
            }
        }

        @Parameterized.Parameters(name = "positive case --- {0}")
        public static Collection<Object[]> data() throws StandardException {
            addCase("digraph {\n" +
                            "A -> B[label=\"Cascade\"];\n" +
                            "A -> B[label=\"Cascade\"];\n" +
                            "}");
            addCase("digraph {\n" +
                            "B -> A[label=\"Cascade\"];\n" +
                            "C -> A[label=\"Cascade\"];\n" +
                            "D -> B[label=\"Cascade\"];\n" +
                            "D -> C[label=\"Cascade\"];\n" +
                            "}");
            addCase("digraph {\n" +
                            "A -> B[label=\"SetNull\"];\n" +
                            "C -> A[label=\"Cascade\"];\n" +
                            "B -> C[label=\"NoAction\"];\n" +
                            "}");
            addCase("digraph {\n" +
                            "D -> A[label=\"SetNull\"];\n" +
                            "D -> C[label=\"Cascade\"];\n" +
                            "C -> B[label=\"Restrict\"];\n" +
                            "B -> A[label=\"Cascade\"];\n" +
                            "}");
            return parameters;
        }

        public PositiveTests(String graph) {
            this.graph = graph;
        }

        String graph;

        @Test
        public void testcase() throws Exception {
            Graph g = new GraphvizParser(graph).generateGraph("something");
            GraphAnnotator annotator = new GraphAnnotator("something", g);
            annotator.annotate();
            annotator.analyzeAnnotations();
        }
    }

    public static class NonParameterizedTest {
        @Test
        public void invalidGraphIsDetectedProperlyCase5() { // invalid cascade cycle
            try {
                new GraphvizParser
                        ("digraph {\n" +
                                 "A -> B[label=\"Cascade\"];\n" +
                                 "B -> C[label=\"Cascade\"];\n" +
                                 "C -> A[label=\"Cascade\"];\n" +
                                 "}").generateGraph("something");
                Assert.fail("expected graph creation to fail with error message: adding the constraint between A and C would cause the following " +
                                    "illegal delete action cascade cycle A B C");
            } catch (Exception e) {
                Assert.assertTrue(e instanceof StandardException);
                StandardException sqlException = (StandardException) e;
                Assert.assertEquals("42915", sqlException.getSQLState());
                Assert.assertTrue(sqlException.getMessage().contains("adding the constraint between A and C would cause the following illegal delete action cascade cycle A B C"));
            }
        }
    }
}
