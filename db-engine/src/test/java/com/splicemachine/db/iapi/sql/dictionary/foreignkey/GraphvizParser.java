package com.splicemachine.db.iapi.sql.dictionary.foreignkey;

import com.splicemachine.db.iapi.error.StandardException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class GraphvizParser {

    private static class Ast {
        private static class Node {
            final String from;
            final String to;
            final EdgeNode.Type type;

            private Node(String from, String to, EdgeNode.Type type) {
                this.from = from;
                this.to = to;
                this.type = type;
            }
        }
        List<Node> nodes = new ArrayList<Node>();

        Set<String> getNodes() {
            Set<String> uniques = new HashSet<String>(nodes.size() * 2);
            for(Node node : nodes) {
                uniques.add(node.from);
                uniques.add(node.to);
            }
            return uniques;
        }
    }

    String input;
    int offset;
    Ast ast;

    public GraphvizParser(String input) {
        this.input = input.replaceAll("\\s", "");
        this.offset = 0;
        ast = new Ast();
    }

    private void parse() {
        parseDigraph();
    }

    private Ast.Node parseEdgeStatement() {
        String from = consumeAlphanumeric();
        parseEdgeRHS();
        String to = consumeAlphanumeric();
        EdgeNode.Type type = consumeLabel();
        return new Ast.Node(from, to, type);
    }

    void consume(String token) {
        if(input.length() < offset + token.length()) {
            throw new IllegalArgumentException("unexpected end of tokens, expected to find: '" + token + "' but the token stream is empty");
        }
        if(!input.startsWith(token,offset)) {
            throw new IllegalArgumentException(String.format("unexpected value, found '%s' expected '%s", input.substring(offset, token.length()), token));
        }
        offset += token.length();
    }

    private void parseDigraph() {
        consume("digraph");
        consume("{");
        parseStatementList();
        consume("}");
    }

    private void parseStatementList() {
        while(!peekChar().equals('}')) {
            ast.nodes.add(parseEdgeStatement());
        }
    }

    EdgeNode.Type consumeLabel() {
        consume("[");
        consume("label");
        consume("=");
        consume("\"");
        String type = consumeAlphanumeric();
        EdgeNode.Type result = EdgeNode.Type.valueOf(type);
        consume("\"");
        consume("]");
        consume(";");
        return result;
    }

    String consumeAlphanumeric() {
        String[] result = input.substring(offset).split("[^a-zA-Z0-9]+", 2);
        consume(result[0]);
        return result[0];
    }

    Character peekChar() {
        if(offset >= input.length()) {
            throw new IllegalArgumentException("unexpected end of tokens");
        }
        return input.charAt(offset);
    }

    private void parseEdgeRHS() {
        consume("->");
    }

    public Graph generateGraph(String constraintName) throws StandardException {
        parse();
        Graph g = new Graph(ast.getNodes(), constraintName);
        for(Ast.Node node : ast.nodes) {
            g.addEdge(node.from, node.to, node.type);
        }
        return g;
    }
}
