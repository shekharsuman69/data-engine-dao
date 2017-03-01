package com.abc.de.hbase.dao;

import java.io.Serializable;
import java.util.Comparator;

/**
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 */

public abstract class HBaseResultComparator implements Comparator<HBaseResultSet>, Serializable {
	private static final long serialVersionUID = 20160523010L;
	protected boolean descending = false;

	public abstract int compare(HBaseResultSet rec1, HBaseResultSet rec2);

	public boolean isDescending() {
		return descending;
	}

	public void setDescending(boolean descending) {
		this.descending = descending;
	}
}
