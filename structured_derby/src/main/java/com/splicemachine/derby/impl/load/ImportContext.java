package com.splicemachine.derby.impl.load;

import com.google.common.base.Preconditions;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.hadoop.fs.Path;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Context information for how to import a File into Splice.
 *
 * @author Scott Fines
 * Created: 2/1/13 11:08 AM
 */
public class ImportContext implements Externalizable{
	private static final long serialVersionUID = 2l;
	//the path to the file to import
	private Path filePath;
	//the delimiter which separates columns
	private String columnDelimiter;
	/*
	 * Some CSV-files have quotes in all their columns, or some
	 * other kind of string which needs to be stripped before it can be
	 * properly serialized. This is the stripString to use
	 */
	private String stripString;
	/*
	 * The types of each column, as java.sql.Types ints.
	 */
	private int[] columnTypes;
	//the conglom id
	private long tableId;

	/*
	 * It's possible to only import certain columns out of the file.
	 * This bitset indicates which columns to use. If it's null, then all
	 * columns will be imported
	 */
	private FormatableBitSet activeCols;

	/*
	 * Not everyone formats their timestamps the same way. This is so that
	 * we can be told how to format them. null can be specified if your timestamps
	 * are formatted according to the default (yyyy-MM-dd HH:mm:ss[.fffffffff]), or if
	 * there are no timestamps in the file to be imported.
	 */
	private String timestampFormat;

	public ImportContext(){}

	private ImportContext(Path filePath,
												long destTableId,
												String columnDelimiter,
												String stripString,
												int [] columnTypes,
												FormatableBitSet activeCols,
												String timestampFormat){
		this.filePath = filePath;
		this.columnDelimiter = columnDelimiter;
		this.stripString = stripString;
		this.columnTypes = columnTypes;
		this.tableId = destTableId;
		this.activeCols = activeCols;
		this.timestampFormat = timestampFormat;
	}

	public Path getFilePath() {
		return filePath;
	}

	public String getColumnDelimiter() {
		return columnDelimiter;
	}

	public String getStripString() {
		return stripString;
	}

	public int[] getColumnTypes() {
		return columnTypes;
	}

	public long getTableId() {
		return tableId;
	}

    public String getTableName(){
        return Long.toString(tableId);
    }

	public FormatableBitSet getActiveCols() {
		return activeCols;
	}

	public String getTimestampFormat() {
		return timestampFormat;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeUTF(filePath.toString());
		out.writeLong(tableId);
		out.writeUTF(columnDelimiter);
		out.writeBoolean(stripString!=null);
		if(stripString!=null)
			out.writeUTF(stripString);
		out.writeInt(columnTypes.length);
		for(int colType:columnTypes){
			out.writeInt(colType);
		}
		out.writeBoolean(activeCols!=null);
		if(activeCols!=null)
			out.writeObject(activeCols);
		out.writeBoolean(timestampFormat!=null);
		if(timestampFormat!=null)
			out.writeUTF(timestampFormat);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		filePath = new Path(in.readUTF());
		tableId = in.readLong();
		columnDelimiter = in.readUTF();
		if(in.readBoolean())
			stripString = in.readUTF();
		columnTypes = new int[in.readInt()];
		for(int i=0;i<columnTypes.length;i++){
			columnTypes[i] = in.readInt();
		}
		if(in.readBoolean())
			activeCols = (FormatableBitSet) in.readObject();
		if(in.readBoolean())
			timestampFormat = in.readUTF();
	}

	@Override
	public String toString() {
		return "ImportContext{" +
				"filePath=" + filePath +
				", columnDelimiter='" + columnDelimiter + '\'' +
				", stripString='" + stripString + '\'' +
				", columnTypes=" + Arrays.toString(columnTypes) +
				", tableId=" + tableId +
				", activeCols=" + activeCols +
				", timestampFormat='" + timestampFormat + '\'' +
				'}';
	}

	public static class Builder{
		private Path filePath;
		private Long tableId;
		private String columnDelimiter;
		private String stripString;
		Map<Integer,Integer> indexToTypeMap = new HashMap<Integer, Integer>();
		private FormatableBitSet activeCols;
		private String timestampFormat;
		private int numCols;

		public Builder path(Path filePath) {
			this.filePath = filePath;
			return this;
		}

		public Builder path(String path){
			this.filePath = new Path(path);
			return this;
		}

		public Builder destinationTable(long tableId) {
			this.tableId = tableId;
			return this;
		}

		public Builder colDelimiter(String columnDelimiter) {
			this.columnDelimiter = columnDelimiter;
			return this;
		}

		public Builder stripCharacters(String stripString) {
			this.stripString = stripString;
			return this;
		}

		public Builder numColumns(int numCols){
			this.numCols = numCols;
			return this;
		}

		public Builder column(int position, int type){
			indexToTypeMap.put(position,type);
			return this;
		}

		public Builder timestampFormat(String timestampFormat) {
			this.timestampFormat = timestampFormat;
			return this;
		}

		public ImportContext build(){
			Preconditions.checkNotNull(filePath,"No File specified!");
			Preconditions.checkNotNull(tableId,"No destination table specified!");
			Preconditions.checkNotNull(columnDelimiter,"No column Delimiter specified");
			Preconditions.checkArgument(numCols>0, "No Columns to import specified!");

			int[] colTypes = new int[numCols];

			FormatableBitSet setBits = new FormatableBitSet(numCols);
			boolean isSparse = false;
			for(int i=0;i<colTypes.length;i++){
				Integer next = indexToTypeMap.get(i);
				if(next!=null){
					colTypes[i] = next;
					setBits.set(i);
				}else{
					isSparse=true;
				}
			}
			if(isSparse){
				activeCols = setBits;
			}

			return new ImportContext(filePath,tableId,
																columnDelimiter,stripString,
																colTypes,activeCols,
																timestampFormat);
		}
	}
}
