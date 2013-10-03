package com.splicemachine.test.diff;

import difflib.myers.Equalizer;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * An implementation of {@link difflib.myers.Equalizer} that performs custom
 * comparison of lines in derby / splice test output that we'd like to massage.
 */
public class LineEqualizer implements Equalizer<String> {
    private static final Logger LOG = Logger.getLogger(LineEqualizer.class);

    // Regex pattern denoting full line of only dashes ('-')
    private static Pattern dashedLinePattern = Pattern.compile("-+");
    private static Matcher dashedLineMatcher = dashedLinePattern.matcher("");

    private static final String CONNECTION_STR = "CONNECTION";
    private static final String IGNORE_START = "splicetest: ignore-order start";
    private static final String IGNORE_STOP = "splicetest: ignore-order stop";
    private static final String CONSTRAINT_ERROR = "ERROR X0Y44: Constraint 'SQL";

    // "in ignore" state flag
    private boolean ignore;
    // lines that will be compared after undergoing sort
    private List<String> derbyOrderIgnore = new ArrayList<String>();
    private List<String> spliceOrderIgnore = new ArrayList<String>();

    /**
     * Override {@link Equalizer#equals(Object)} so we can get our custom behavior.
     * <p>
     *     This method attempts use the default behavior (<code>line1.equals(line2)</code>)
     *     where it can and only checks lines with custom behavior if there's a diff.<br/>
     *     The notable exception to that is when we're observing a "start ignore" directive
     *     - we have to go line-by-line until we observe a "stop ignore" directive in that
     *     case.
     * </p>
     *
     * @param derbyLine the line that comes from a derby output file. Will be trimmed.
     * @param spliceLine the line that comes from a splice output file. Will be trimmed.
     * @return <code>true</code> if the two output lines compare equal after customizations
     * are applied.
     */
    @Override
    public boolean equals(String derbyLine, String spliceLine) {
        String derbyTrimmed = derbyLine.trim();
        String spliceTrimmed = spliceLine.trim();

        //======================
        // ignore-order rules
        // Ignore the order, but not the content, of a set of line
        // between "-- splicetest: ignore-order start/stop" directives
        // in the output files.
        //======================
        if (ignore) {
            // add lines that will be sorted then compared
            pushIgnoredLines(derbyTrimmed, spliceTrimmed);

            // we're in an ignore directive ("order by" result ordered differently)
            if (derbyTrimmed.contains(IGNORE_STOP) ||
                    spliceTrimmed.contains(IGNORE_STOP)) {
                // we've found an ignore "stop" directive
                // ...stop ignoring line diffs
                LOG.debug("Found [" + IGNORE_STOP + "] no longer ignoring lines...");
                ignore = false;

                // compare lines we've been saving
                return compareIgnoredLines();
            }
            return true;
        }
        if (derbyTrimmed.contains(IGNORE_START) &&
                spliceTrimmed.contains(IGNORE_START)) {
            // we've found an ignore directive ("order by" result ordered differently)
            // ...ignore all lines until we see an ignore "stop" directive
            LOG.debug("Found [" + IGNORE_START + "] ignoring lines...");
            ignore = true;

            // add lines that will be sorted then compared
            pushIgnoredLines(derbyTrimmed, spliceTrimmed);

            return true;
        }

        //======================
        // default impl; string compare
        //
        // We do default compare ASAP and only if we have a diff do we check further
        // with our customizations
        //======================
        boolean isSame = derbyTrimmed.equals(spliceTrimmed);

        if (!isSame) {
            // do expensive, less frequent occurrences here...

            //======================
            // constraint error rule
            //======================
            if (derbyTrimmed.startsWith(CONSTRAINT_ERROR) &&
                    spliceTrimmed.startsWith(CONSTRAINT_ERROR)) {
                // Not a bug, chars appearing after "SQL" are a conglomerate ID - different for each DB instance
                return true;
            }

            //======================
            // ignore whitespace rule
            //======================
            if (derbyTrimmed.contains("|") && spliceTrimmed.contains("|")) {
                // Case where formatted output is formatted differently by Derby and Splice:
                //<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
                //Users/jeff/dev/spliceengine/structured_test/target/nist/dml130.derby
                //Position 79:
                //  [1               |NU&|NUM1C3]
                //  [0.00            |0  |0     ]
                //++++++++++++++++++++++++++
                //Users/jeff/dev/spliceengine/structured_test/target/nist/dml130.splice
                //Position 79:
                //  [1              |NU&|NUM1C3]
                //  [0.00           |0  |0     ]
                //>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>
                // Compare all non-whitespace tokens
                return equalsIgnoreWhitespace(derbyTrimmed, spliceTrimmed);
            }

            //======================
            // ignore "CONNECTION" rule
            //======================
            if (derbyTrimmed.startsWith(CONNECTION_STR) && spliceTrimmed.startsWith(CONNECTION_STR)) {
                // Derby and Splice connection strings will always be different
                return true;
            }

            //======================
            // ignore dashed line rule. We ignore lines with only dashes, even if
            // they have different lengths.
            // (last because matcher.reset(string) may be expensive)
            //======================
            if (bothLinesCompleteDashes(derbyTrimmed, spliceTrimmed)) {
            	return true;
            }

        }

        return isSame;
    }

    private boolean compareIgnoredLines() {
		// The problem with this stateful approach is, by the time we have
		// a false equals, we're way past the line where the problem occurred.
		// Log something so we can find our way to the real prob

        try {
        	if (! (this.derbyOrderIgnore.size() == this.spliceOrderIgnore.size())) {
        		return logAndReturnFalse(this.derbyOrderIgnore, this.spliceOrderIgnore);
        	}
        	// sort and compare each element
        	List<String> derbySorted = new ArrayList<String>(this.derbyOrderIgnore);
        	List<String> spliceSorted = new ArrayList<String>(this.spliceOrderIgnore);
			Collections.sort(derbySorted);
			Collections.sort(spliceSorted);
			for (int i = 0; i<derbySorted.size(); i++) {
				String derbyLine = derbySorted.get(i);
				String spliceLine = spliceSorted.get(i);
				boolean equal = derbyLine.equals(spliceLine);
				if (! equal) {
					// ignore if both lines contain only dashes
					if (! bothLinesCompleteDashes(derbyLine, spliceLine)) {
						if (derbyLine.contains("|") && spliceLine.contains("|")) {
							// remove WS formatting and compare
							equal = equalsIgnoreWhitespace(derbyLine, spliceLine);
						}
					}
				}
				if (! equal) {
					return logAndReturnFalse(this.derbyOrderIgnore, this.spliceOrderIgnore);
				}
			}
		} finally {
			this.derbyOrderIgnore.clear();
			this.spliceOrderIgnore.clear();
		}
        return true;
    }
    
    private static boolean logAndReturnFalse(List<String> derbyLines, List<String> spliceLines) {
		LOG.error("Order-ignored lines don't compare even after sorting.\nDerby:\n"
				+ derbyLines + "\nSplice:\n" + spliceLines);
		return false;
    }

    private void pushIgnoredLines(String derby, String splice) {
        this.derbyOrderIgnore.add(derby);
        this.spliceOrderIgnore.add(splice);
    }
    
    private static boolean bothLinesCompleteDashes(String derby, String splice) {
        // if both lines contain only dashes ('-'), ignore (return match == true)
        return (dashedLineMatcher.reset(derby).matches() && dashedLineMatcher.reset(splice).matches());
    }

    private static boolean equalsIgnoreWhitespace(String derby, String splice) {
        String[] derbyTokens = derby.split("\\s+");
        String[] spliceTokens = splice.split("\\s+");
		if (derbyTokens.length != spliceTokens.length) {
			return false;
		} else {
			int i = 0;
			while (i < derbyTokens.length) {
				if (!derbyTokens[i].equals(spliceTokens[i])) {
					return false;
				}
				++i;
			}
		}
		return true;
    }
}
