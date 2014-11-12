package com.splicemachine.derby.impl.load;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.splicemachine.derby.utils.StringUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.fs.Path;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;

/**
 * Context information for how to import a File into Splice.
 *
 * @author Scott Fines
 * Created: 2/1/13 11:08 AM
 */
public class ImportContext implements Externalizable{
		private static final long serialVersionUID = 3l;
		protected static final String DEFAULT_COLUMN_DELIMITTER = ",";
		protected static final String DEFAULT_STRIP_STRING = "\"";


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
		//the conglom id
		private long tableId;

		private ColumnContext[] columnInformation;
		/*
		 * Not everyone formats their timestamps the same way. This is so that
		 * we can be told how to format them. null can be specified if your timestamps
		 * are formatted according to the default (yyyy-MM-dd HH:mm:ss[.fffffffff]), or if
		 * there are no timestamps in the file to be imported.
		 */
		private String timestampFormat;
		private String dateFormat;
		private String timeFormat;

		private long byteOffset;
		private int bytesToRead;

		private boolean recordStats;

		/*
		 * The maximum number of records that can fail irretrievably before failing
		 * the import task
		 */
		private long maxBadRecords;
		private Path badLogDirectory;
		private String tableVersion;

    private boolean isUpsert;

    public ImportContext(){}

		private ImportContext( Path filePath,
													long destTableId,
													String columnDelimiter,
													String stripString,
													ColumnContext[] columnInformation,
													String timestampFormat,
													String dateFormat,
													String timeFormat,
													long byteOffset,
													int bytesToRead,
													boolean recordStats,
													long maxBadRecords,
													Path badLogDirectory,
													String tableVersion,
                          boolean isUpsert){
				this.filePath = filePath;
				this.columnDelimiter = columnDelimiter!= null?columnDelimiter:DEFAULT_COLUMN_DELIMITTER;
				this.stripString = stripString!=null?stripString:DEFAULT_STRIP_STRING;
				this.tableId = destTableId;
				this.timestampFormat = timestampFormat;
				this.timeFormat = timeFormat;
				this.dateFormat = dateFormat;
				this.byteOffset = byteOffset;
				this.bytesToRead = bytesToRead;
				this.columnInformation = columnInformation;
				this.recordStats = recordStats;
				this.maxBadRecords = maxBadRecords;
				this.badLogDirectory = badLogDirectory;
				this.tableVersion = tableVersion;
        this.isUpsert = isUpsert;
		}

		public void setFilePath(Path filePath) {
				this.filePath = filePath;
		}

		public String getColumnDelimiter() {
				String delim = columnDelimiter;
				if(delim==null||delim.length()<=0)
						delim = ",";
				return delim;
		}

		public String getQuoteChar(){
				String stripStr = getStripString();
				if(stripStr==null||stripStr.length()<=0)
						stripStr = "\"";
				return stripStr;
		}

		public Path getFilePath() { return filePath; }
		public String getStripString() { return stripString; }
		public long getTableId() { return tableId; }
		public String getTableName(){ return Long.toString(tableId); }
		public String getTimestampFormat() { return timestampFormat; }
		public long getByteOffset() { return byteOffset; }
		public int getBytesToRead() { return bytesToRead; }
		public String getDateFormat() { return dateFormat; }
		public String getTimeFormat() { return timeFormat; }
		public ColumnContext[] getColumnInformation() { return columnInformation; }
		public boolean shouldRecordStats() { return recordStats; }

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
				out.writeUTF(filePath.toString());
				out.writeLong(tableId);
				out.writeUTF(columnDelimiter);
				out.writeBoolean(stripString!=null);
				if(stripString!=null)
						out.writeUTF(stripString);
				out.writeInt(columnInformation.length);
				for(ColumnContext context:columnInformation){
						out.writeObject(context);
				}
				out.writeBoolean(timestampFormat!=null);
				if(timestampFormat!=null)
						out.writeUTF(timestampFormat);
				out.writeBoolean(dateFormat!=null);
				if(dateFormat!=null)
						out.writeUTF(dateFormat);
				out.writeBoolean(timeFormat!=null);
				if(timeFormat!=null)
						out.writeUTF(timeFormat);
				out.writeLong(byteOffset);
				out.writeInt(bytesToRead);
				out.writeBoolean(recordStats);
				out.writeLong(maxBadRecords);
				out.writeBoolean(badLogDirectory!=null);
				if(badLogDirectory!=null)
						out.writeUTF(badLogDirectory.toString());

				out.writeBoolean(tableVersion!=null);
				if(tableVersion!=null)
						out.writeUTF(tableVersion);
        out.writeBoolean(isUpsert);
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
				filePath = new Path(in.readUTF());
				tableId = in.readLong();
				columnDelimiter = in.readUTF();
				if(in.readBoolean())
						stripString = in.readUTF();
				columnInformation = new ColumnContext[in.readInt()];
				for(int i=0;i<columnInformation.length;i++){
						columnInformation[i] = (ColumnContext)in.readObject();
				}
				if(in.readBoolean())
						timestampFormat = in.readUTF();
				if(in.readBoolean())
						dateFormat = in.readUTF();
				if(in.readBoolean())
						timeFormat = in.readUTF();
				byteOffset = in.readLong();
				bytesToRead = in.readInt();
				recordStats = in.readBoolean();
				maxBadRecords = in.readLong();
				if(in.readBoolean())
						badLogDirectory = new Path(in.readUTF());
				if(in.readBoolean())
						tableVersion = in.readUTF();
        isUpsert = in.readBoolean();
		}

		@Override
		public String toString() {
				return "ImportContext{" +
								", filePath=" + filePath +
								", columnDelimiter='" + columnDelimiter + '\'' +
								", stripString='" + stripString + '\'' +
								", timestampFormat='" + timestampFormat + '\'' +
								", dateFormat='" + dateFormat + '\'' +
								", timeFormat='" + timeFormat + '\'' +
								", byteOffset=" + byteOffset +
								", bytesToRead=" + bytesToRead +
								", columns="+ Arrays.toString(columnInformation) +
                ", isUpsert="+isUpsert+
								'}';
		}


		public int[] getPrimaryKeys() {
				int[] pkCols = new int[columnInformation.length];
				Arrays.fill(pkCols,-1);
				int setFields =0;
				for(ColumnContext columnContext:columnInformation){
						if(columnContext.isPkColumn()){
								setFields++;
								pkCols[columnContext.getPkPosition()] = columnContext.getColumnNumber();
						}
				}

				int[] finalPks = new int[setFields];
				System.arraycopy(pkCols,0,finalPks,0,finalPks.length);

				return finalPks;
		}


		public ImportContext getCopy() {
				return new ImportContext(filePath,tableId,columnDelimiter,stripString,
								columnInformation,timestampFormat,
								dateFormat,timeFormat,byteOffset,bytesToRead,
								recordStats,maxBadRecords,badLogDirectory,tableVersion,isUpsert);
		}

		public long getMaxBadRecords() { return maxBadRecords; }

		public Path getBadLogDirectory() { return badLogDirectory; }

		public String getTableVersion() { return tableVersion; }

    public boolean isUpsert() {
        return isUpsert;
    }

    public static class Builder{
				private Path filePath;
				private Long tableId;
				private String columnDelimiter = null;
				private String stripString;
				Map<Integer,Integer> indexToTypeMap = new HashMap<Integer, Integer>();
				private String timestampFormat;
				private long byteOffset;
				private int bytesToRead;

				private String timeFormat;
				private String dateFormat;

				private List<ColumnContext> columnInformation = Lists.newArrayList();
				private boolean recordStats = false;

				private long maxBadRecords = 0l;
				private Path badLogDirectory = null;
				private String tableVersion;
        private boolean isUpsert = false;

        public Builder upsert(boolean upsert){
            this.isUpsert = upsert;
            return this;
        }

        public Builder maxBadRecords(long maxBadRecords){
						this.maxBadRecords = maxBadRecords;
						return this;
				}

				public Builder badLogDirectory(Path badLogDirectory){
						this.badLogDirectory = badLogDirectory;
						return this;
				}


				public Builder addColumn(ColumnContext columnContext){
						this.columnInformation.add(columnContext);
						return this;
				}

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
            if(columnDelimiter==null) return this; //use the default value for column delimiter

						String colDelim = StringUtils.parseControlCharacters(columnDelimiter);
						if(System.getProperty("line.separator").equals(colDelim)){
								throw new AssertionError("cannot use linebreaks as column separators");
						}

						//ensure that the System
						this.columnDelimiter = colDelim;
						return this;
				}

				public Builder stripCharacters(String stripString) {
						this.stripString = (stripString == null ? null : StringUtils.parseControlCharacters(stripString));
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

				public Builder timeFormat(String timeFormat){
						this.timeFormat = timeFormat;
						return this;
				}

				public Builder dateFormat(String dateFormat){
						this.dateFormat = dateFormat;
						return this;
				}

				public Builder byteOffset(long byteOffset){
						this.byteOffset = byteOffset;
						return this;
				}

				public Builder bytesToRead(int bytesToRead){
						this.bytesToRead = bytesToRead;
						return this;
				}

				public Builder recordStats(){
						this.recordStats = true;
						return this;
				}

				public Builder tableVersion(String tableVersion){
						this.tableVersion = tableVersion;
						return this;
				}

				public ImportContext build() throws StandardException {
						Preconditions.checkNotNull(filePath,"No File specified!");
						Preconditions.checkNotNull(tableId,"No destination table specified!");

            if(columnDelimiter==null)
                columnDelimiter = ",";

						ColumnContext[] context = new ColumnContext[columnInformation.size()];
						Collections.sort(columnInformation,new Comparator<ColumnContext>() {
								@Override
								public int compare(ColumnContext o1, ColumnContext o2) {
										if(o1==null){
												if(o2==null) return 0;
												return -1;
										}else if(o2==null)
												return 1;

										return o1.getColumnNumber()-o2.getColumnNumber();
								}
						});
						columnInformation.toArray(context);
						return new ImportContext(filePath,tableId,
										columnDelimiter,stripString,
										context,
										timestampFormat,dateFormat,timeFormat, byteOffset, bytesToRead,
										recordStats,maxBadRecords,badLogDirectory,tableVersion,isUpsert);
				}

				public long getDestinationConglomerate() {
						return this.tableId;
				}

    }
}
