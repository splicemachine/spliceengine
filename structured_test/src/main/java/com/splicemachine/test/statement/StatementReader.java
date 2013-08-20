package com.splicemachine.test.statement;

/**
 * @author Jeff Cunningham
 *         Date: 7/23/13
 */

import org.apache.derby.iapi.tools.i18n.LocalizedInput;
import org.apache.derby.iapi.tools.i18n.LocalizedOutput;
import org.apache.derby.impl.tools.ij.StatementFinder;

import java.io.FileInputStream;
import java.io.OutputStream;

public class StatementReader {
    private final StatementFinder sf;

    public StatementReader(FileInputStream in, OutputStream out) {
        this.sf = new StatementFinder(new LocalizedInput(in), new LocalizedOutput(out));
    }

    public String nextStatement() {
        return sf.nextStatement();
    }

//    public static void main(String... args) throws Exception {
//        FileInputStream is = new FileInputStream(args[0]);
//        StatementFinder finder = new StatementFinder(is);
//
//        while (true) {
//            String statement = finder.nextStatement();
//            if (statement == null) {
//                break;
//            }
//
//            System.out.println(statement);
//            System.out.println("------------------");
//        }
//
//    }
}