package com.abc.de.hbase.dao;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class HBaseResultSet implements Serializable {
	private static final long serialVersionUID = 20160523005L;
	private Result result;

	public HBaseResultSet() {
	}

	public HBaseResultSet(Result value) {
		result = value;
	}

	public String getRowKey() {
		byte[] row = result.getRow();
		if (row == null)
			return null;
		return Bytes.toString(row);
	}

	public Object getObject(String columFamily, String columnName) throws Exception {
		Object obj = null;
		byte[] bytes = getByteArray(columFamily, columnName);
		if (bytes == null)
			return null;

		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
			ObjectInputStream ois = new ObjectInputStream(bis);
			obj = ois.readObject();
			ois.close();
			bis.close();
		} catch (IOException ex) {
			throw new Exception();
		} catch (ClassNotFoundException ex) {
			throw new Exception();
		}
		return obj;
	}

	public byte[] getByteArray(String columFamily, String columnName) {
		if (result == null)
			return null;
		return result.getValue(Bytes.toBytes(columFamily), Bytes.toBytes(columnName));
	}

	public String getString(String columFamily, String columnName) {
		byte[] data = getByteArray(columFamily, columnName);
		if (data == null)
			return null;
		return Bytes.toString(data);
	}

	public boolean getBoolean(String columFamily, String columnName) {
		byte[] data = getByteArray(columFamily, columnName);
		if (data == null)
			return false;
		return Bytes.toBoolean(data);
	}

	public int getInt(String columFamily, String columnName) {
		byte[] data = getByteArray(columFamily, columnName);
		if (data == null)
			return -99999999;
		return Bytes.toInt(data);
	}

	public long getLong(String columFamily, String columnName) {
		byte[] data = getByteArray(columFamily, columnName);
		if (data == null)
			return -99999999L;
		return Bytes.toLong(data);
	}

	public double getDouble(String columFamily, String columnName) {
		byte[] data = getByteArray(columFamily, columnName);
		if (data == null)
			return -99999999.0;
		return Bytes.toDouble(data);
	}

	public short getShort(String columFamily, String columnName) {
		byte[] data = getByteArray(columFamily, columnName);
		if (data == null)
			return -999;
		return Bytes.toShort(data);
	}

	public BigDecimal getBigDecimal(String columFamily, String columnName) {
		byte[] data = getByteArray(columFamily, columnName);
		if (data == null)
			return null;
		return Bytes.toBigDecimal(data);
	}

	public Result getResult() {
		return result;
	}

	public String toString() {
		if (null == result)
			return "";
		StringBuffer sb = new StringBuffer();
		List<Cell> cells = result.listCells();
		if (cells != null) {
			Iterator<Cell> iterator = cells.iterator();
			while (iterator.hasNext()) {
				Cell cell = iterator.next();
				sb.append(cell);
			}
		}
		return sb.toString();

	}

	/**
	 * Return the array of Cells backing this Result instance. This API is
	 * slower than using rawCells()
	 * 
	 * @param columnFamily
	 * @return
	 */
	public String[] getColumnsInColumnFamily(String columnFamily) {
		String[] quantifers = null;

		NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(Bytes.toBytes(columnFamily));

		if (familyMap != null && familyMap.size() > 0) {
			quantifers = new String[familyMap.size()];

			int counter = 0;
			for (byte[] bQunitifer : familyMap.keySet()) {
				quantifers[counter++] = Bytes.toString(bQunitifer);
			}
		}
		return quantifers;
	}

}