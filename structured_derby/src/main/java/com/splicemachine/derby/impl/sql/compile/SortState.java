package com.splicemachine.derby.impl.sql.compile;

public interface SortState {
	void setNumberOfRegions(int numberOfRegions);
	int getNumberOfRegions();
}
