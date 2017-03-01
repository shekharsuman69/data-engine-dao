package com.abc.de.hbase.dao;

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class HBaseRow implements Comparable<HBaseRow>, Serializable {
	private static final long serialVersionUID = 20160523004L;
	private String rowKey;
	private List<HBaseCell> cells = new ArrayList<>();

	public HBaseRow() {
	}

	public HBaseRow(String rowkey) {
		this.rowKey = rowkey;
	}

	public void addColumnData(String columnFamily, String columnName, String data) {
		addColumnData(columnFamily, columnName, data == null ? HConstants.EMPTY_BYTE_ARRAY : Bytes.toBytes(data));
	}

	public void addColumnData(String columnFamily, String columnName, boolean data) {
		addColumnData(columnFamily, columnName, Bytes.toBytes(data));
	}

	public void addColumnData(String columnFamily, String columnName, int data) {
		addColumnData(columnFamily, columnName, Bytes.toBytes(data));
	}

	public void addColumnData(String columnFamily, String columnName, long data) {
		addColumnData(columnFamily, columnName, Bytes.toBytes(data));
	}

	public void addColumnData(String columnFamily, String columnName, double data) {
		addColumnData(columnFamily, columnName, Bytes.toBytes(data));
	}

	public void addColumnData(String columnFamily, String columnName, short data) {
		addColumnData(columnFamily, columnName, Bytes.toBytes(data));
	}

	public void addColumnData(String columnFamily, String columnName, BigDecimal data) {
		addColumnData(columnFamily, columnName, data == null ? HConstants.EMPTY_BYTE_ARRAY : Bytes.toBytes(data));
	}

	public void addColumnData(String columnFamily, String columnName, Object data) throws java.io.IOException {
		addColumnData(columnFamily, columnName, getBytes(data));
	}

	public void addColumnData(String columnFamily, String columnName, byte[] data) {
		cells.add(new HBaseCell(columnFamily, columnName, data));
	}

	private byte[] getBytes(Object obj) throws java.io.IOException {
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(obj);
		oos.flush();
		oos.close();
		bos.close();
		byte[] data = bos.toByteArray();
		return data;
	}

	public String getRowKey() {
		return rowKey;
	}

	public void setRowKey(String rowKey) {
		this.rowKey = rowKey;
	}

	public List<HBaseCell> getCells() {
		return cells;
	}

	public int compareTo(HBaseRow compareRow) {

		if (compareRow == null)
			return 1;
		return this.rowKey.compareTo(compareRow.rowKey);
	}

	public String toString() {
		StringBuffer sb = new StringBuffer();
		sb.append("[rowKey=");
		sb.append(rowKey);
		sb.append("; data =");
		boolean first = true;
		if (cells != null) {
			Iterator<HBaseCell> iterator = cells.iterator();
			while (iterator.hasNext()) {
				if (!first)
					sb.append(", ");
				sb.append(iterator.next());
				first = false;
			}
		}
		sb.append("]");
		return sb.toString();
	}
}
