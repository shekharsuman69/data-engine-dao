package com.abc.de.hbase.dao;

import java.io.Serializable;

/**
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 * 
 */
public class HBaseCell extends HBaseColumn implements Serializable {
	private static final long serialVersionUID = 20160523003L;
	private byte[] data;

	public HBaseCell(String cf, String cn, byte[] bytes) {
		super(cf, cn);
		data = bytes;
	}

	public byte[] getData() {
		return data;
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("{");
		sb.append("columnFamily=");
		sb.append(columnFamily);
		sb.append("; columnName=");
		sb.append(columnName);
		sb.append("; data=");
		try {
			sb.append(new String(data, "UTF-8"));
		} catch (Exception e) {
		}
		sb.append("}");
		return sb.toString();
	}
}
