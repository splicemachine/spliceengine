package com.splicemachine.test.verify;

import java.io.File;
import java.util.List;

/**
 * @author Jeff Cunningham
 *         Date: 10/3/13
 */
public interface Verifier {

    /**
     * Given a list of files, verify tests succeeded and
     * generate a report.
     * @param files files to verify
     * @return a report for each file
     */
    List<VerifyReport> verifyOutput(List<File> files);
}
