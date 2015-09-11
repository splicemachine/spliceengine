package com.splicemachine.derby.impl.load;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ITokenizer;
import org.supercsv.prefs.CsvPreference;

/**
 * SpliceCsvReader is a simple reader that reads a row from a CSV file into an array of Strings.
 *
 * @author dwinters
 */
public class SpliceCsvReader extends CsvListReader {

    private ArrayList<String> failMsg = new ArrayList<String>();

	/**
	 * Constructs a new <tt>SpliceCsvReader</tt> with the supplied Reader and CSV preferences. Note that the
	 * <tt>reader</tt> will be wrapped in a <tt>BufferedReader</tt> before accessed.
	 * 
	 * @param reader
	 *            the reader
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if reader or preferences are null
	 */
	public SpliceCsvReader(Reader reader, CsvPreference preferences) {
		super(reader, preferences);
	}

	/**
	 * Constructs a new <tt>SpliceCsvReader</tt> with the supplied (custom) Tokenizer and CSV preferences. The tokenizer
	 * should be set up with the Reader (CSV input) and CsvPreference beforehand.
	 * 
	 * @param tokenizer
	 *            the tokenizer
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if tokenizer or preferences are null
	 */
	public SpliceCsvReader(ITokenizer tokenizer, CsvPreference preferences) {
		super(tokenizer, preferences);
	}

	/**
	 * Reads a row of a CSV file and returns an array of Strings containing each column.
	 * This method primarily exists because SuperCSV uses Lists of Strings and
	 * Splice Machine uses String arrays.  This saves some extraneous object creation.
	 * 
	 * @return the array of columns, or null if EOF
	 * @throws IOException
	 *             if an I/O error occurred
	 * @throws SuperCsvException
	 *             if there was a general exception while reading/processing
	 */
	public String[] readAsStringArray() throws IOException {
        boolean res = false;

        try {
            res = readRow();
        } catch (Exception e) {
            System.out.println("Read CSV-file error:" + e.getMessage());
            //throw e;
            failMsg.add(e.getMessage());
        }

		if (res) {
			List<String> rowAsList = getColumns();
			String[] row = new String[rowAsList.size()];
			rowAsList.toArray(row);
			return row;
		}
		
		return null; // EOF
	}


    public ArrayList<String> getFailMsg() {
        return failMsg;
    }
}
