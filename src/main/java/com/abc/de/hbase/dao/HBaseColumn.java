package com.abc.de.hbase.dao;

import java.io.Serializable;

/**
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class HBaseColumn implements Serializable {
	private static final long serialVersionUID = 20160523002L;
	protected String columnFamily;
	protected String columnName;

	public HBaseColumn(String cf, String cn) {
		columnFamily = cf;
		columnName = cn;
	}

	public String getColumnFamily() {
		return columnFamily;
	}

	public String getColumnName() {
		return columnName;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		sb.append("columnFamily=");
		sb.append(columnFamily);
		sb.append("; columnName=");
		sb.append(columnName);
		sb.append("}");
		return sb.toString();
	}
}
