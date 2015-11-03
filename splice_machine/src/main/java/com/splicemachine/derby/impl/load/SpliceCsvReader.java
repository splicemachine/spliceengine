package com.splicemachine.derby.impl.load;

import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.List;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ITokenizer;
import org.supercsv.prefs.CsvPreference;

/**
 * SpliceCsvReader is a simple reader that reads a row from a CSV file into an array of Strings.
 *
 * @author dwinters
 */
public class SpliceCsvReader extends CsvListReader implements Iterator<List<String>> {

    private String fileName;

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
	public SpliceCsvReader(Reader reader, CsvPreference preferences, String fileName) {
		super(reader, preferences);
        this.fileName = fileName;
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

    @Override
    public boolean hasNext() {
        try {
            return readRow();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public List<String> next() {
        return getColumns();
    }

    @Override
    public void remove() {
        throw new RuntimeException("Not Supported");
    }

}
