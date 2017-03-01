package com.abc.de.hbase.dao;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ClusterStatus;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableMultiplexer;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.abc.de.config.Constants;

/**
 * @author Shekhar Suman
 * @version 1.0
 * @since 2017-03-01
 *
 */
public class HBaseDAO implements Serializable {
	private static final long serialVersionUID = 20160523001L;
	protected static final Logger log = LoggerFactory.getLogger(HBaseDAO.class);
	public static final String DEFAULT_COLUMN_FAMILY = "cf_data";
	private int batchSize = Constants.DEFAULT_HBASE_BATCH_SIZE;
	private long batchInterval = Constants.DEFAULT_HBASE_BATCH_INTERVAL;
	protected static Configuration conf = null;
	protected Connection connection = null;
	protected String tableName = null;
	protected TableName TABLE_NAME = null;
	protected HashMap<String, HBaseRow> rows2Add = new HashMap<>();
	protected Properties properties;
	private long lastUpdate = System.currentTimeMillis();
	private long executionTime = -1l;
	private int rows = 0;
	private static HTableMultiplexer tableMultiplexer;
	private List<Put> pendingPuts = null;

	public HBaseDAO(Properties prop) {
		super();
		properties = prop;
	}

	public HBaseDAO(Properties prop, String tableName) {
		this(prop);
		this.tableName = tableName;
		TABLE_NAME = TableName.valueOf(tableName);
	}

	protected void finalize() throws Throwable {

		try {
			if (rows2Add.size() > 0) {
				write();
				rows2Add = null;
			}
		} catch (Exception e) {
			log.error("Exception caught in HBaseDAO.finalize()", e);
		}
		try {
			cleanup();
		} catch (Exception e) {

		}
		try {
			connection.close();
		} catch (Exception e) {

		} finally {
			super.finalize();
		}
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
		TABLE_NAME = TableName.valueOf(tableName);
	}

	/**
	 * Creates a Put object in an list. It can be invoked multiple times before
	 * calling write().
	 *
	 * write() must be called in order to flush data to HBase
	 */
	public void addRow(HBaseRow row) throws Exception {
		if (tableName == null)
			throw new Exception("tableName is null!");

		if (row == null || row.getRowKey() == null) {
			return;
		}
		if (rows2Add == null)
			rows2Add = new HashMap<>();
		if (rows2Add.containsKey(row.getRowKey())) {
			HBaseRow r = rows2Add.get(row.getRowKey());
			List<HBaseCell> oldCells = r.getCells();
			oldCells.addAll(row.getCells());
		} else
			rows2Add.put(row.getRowKey(), row);
	}

	private List<Put> getPutList(boolean wal) {
		if (pendingPuts == null)
			pendingPuts = new ArrayList<>();
		if (rows2Add.size() > 0) {
			for (HBaseRow row : rows2Add.values()) {
				Put p = new Put(Bytes.toBytes(row.getRowKey()));
				if (!wal)
					p.setDurability(Durability.SKIP_WAL);
				List<HBaseCell> cells = row.getCells();
				if (cells != null) {
					Iterator<HBaseCell> iterator = cells.iterator();
					while (iterator.hasNext()) {
						HBaseCell cell = iterator.next();
						String cfname = cell.getColumnFamily() == null ? DEFAULT_COLUMN_FAMILY : cell.getColumnFamily();
						byte[] columnname = cell.getColumnName() == null ? HConstants.EMPTY_BYTE_ARRAY
								: Bytes.toBytes(cell.getColumnName());
						p.addColumn(Bytes.toBytes(cfname), columnname,
								cell.getData() == null ? HConstants.EMPTY_BYTE_ARRAY : cell.getData());
					}
				}
				pendingPuts.add(p);
			}

			rows2Add.clear();
		}
		return pendingPuts;
	}

	public int getPendingRowSize() {
		return rows2Add.size();
	}

	public void write() throws Exception {
		write(true);
	}

	/**
	 * Inserts rows added through addRow() to HBase
	 * 
	 * Usage example:
	 * 
	 * HBaseDAO writer = new HBaseDAO("test"); HBaseRow row1 = new
	 * HBaseRow("123"); HBaseRow row2 = new HBaseRow("234");
	 * row1.addColumnData("cf_data","col1","xyz");
	 * row1.addColumnData("cf_data","col2","abc");
	 * row2.addColumnData("cf_data","col1","column1");
	 * row2.addColumnData("cf_data","col2","column2"); writer.addRow(row1);
	 * writer.addRow(row2); writer.write();
	 * 
	 */
	public void write(boolean wal) throws Exception {

		try {
			long starttime = System.currentTimeMillis();
			synchronized (this) {
				getPutList(wal);
				if (pendingPuts == null || pendingPuts.size() < 1)
					return;
				if (conf == null)
					configure(properties);
				if (tableMultiplexer == null) {
					tableMultiplexer = new HTableMultiplexer(conf, batchSize * 10);
				}
				rows = pendingPuts.size();
				pendingPuts = tableMultiplexer.put(TABLE_NAME, pendingPuts);
				if (pendingPuts != null) {
					rows = rows - pendingPuts.size();
				}

			}
			executionTime = System.currentTimeMillis() - starttime;
		} catch (Exception e) {
			try {
				tableMultiplexer.close();
			} catch (Exception ex) {
			}
			tableMultiplexer = null;
			e.printStackTrace();
			throw new Exception("Could not insert to  [" + tableName + "][" + e.getMessage() + "]");
		}
	}

	public void writeBatch() throws Exception {

		try {
			long now = System.currentTimeMillis();
			long starttime = now;
			if (rows2Add.size() >= batchSize || (now - lastUpdate) >= batchInterval) {
				write();
				lastUpdate = now;
			}
			executionTime = System.currentTimeMillis() - starttime;
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Exception caught in writeBatch()", e);
		}
	}

	public long incrementValue(String columnFamily, String column, String rowKey, long amount) throws Exception {
		long starttime = System.currentTimeMillis();

		try {
			Table table = getTable(properties, tableName);
			long incr = table.incrementColumnValue(Bytes.toBytes(rowKey), Bytes.toBytes(columnFamily),
					Bytes.toBytes(column), amount);
			table.close();
			return incr;
		} catch (Exception e) {
			throw new Exception(e);
		} finally {
			executionTime = System.currentTimeMillis() - starttime;
		}
	}

	public HBaseResultSet read(String rowkey, HBaseColumn... columns) throws Exception {
		if (tableName == null) {
			throw new Exception("Table name is null!");
		}

		Get get = new Get(Bytes.toBytes(rowkey));
		if (columns == null)
			get.addFamily(HConstants.EMPTY_BYTE_ARRAY);
		else {
			for (HBaseColumn column : columns)
				get.addColumn(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(column.getColumnName()));
		}
		return readRow(get);
	}

	public HBaseResultSet[] read(List<String> keys, HBaseColumn... columns) throws Exception {
		if (tableName == null) {
			throw new Exception("Table name is null!");
		}

		List<Get> getList = new ArrayList<Get>();
		for (String rowkey : keys) {
			Get get = new Get(Bytes.toBytes(rowkey));
			if (columns == null)
				get.addFamily(HConstants.EMPTY_BYTE_ARRAY);
			else {
				for (HBaseColumn column : columns)
					get.addColumn(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(column.getColumnName()));
			}
			getList.add(get);
		}

		return readRow(getList);
	}

	public HBaseResultSet read(String rowkey, String family) throws Exception {
		if (tableName == null) {
			throw new Exception("Table name is null!");
		}

		Get get = new Get(Bytes.toBytes(rowkey));
		if (family == null)
			get.addFamily(HConstants.EMPTY_BYTE_ARRAY);
		else {
			get.addFamily(Bytes.toBytes(family));
		}

		return readRow(get);
	}

	private HBaseResultSet readRow(Get get) throws Exception {
		long starttime = System.currentTimeMillis();
		try {
			Table table = getTable(properties, tableName);
			Result result = table.get(get);
			table.close();
			return new HBaseResultSet(result);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		} finally {
			executionTime = System.currentTimeMillis() - starttime;
		}
	}

	public HBaseResultSet[] read(List<String> keys, String family) throws Exception {
		if (tableName == null) {
			throw new Exception("Table name is null!");
		}

		List<Get> getList = new ArrayList<Get>();
		for (String rowkey : keys) {
			Get get = new Get(Bytes.toBytes(rowkey));
			if (family == null)
				get.addFamily(HConstants.EMPTY_BYTE_ARRAY);
			else {
				get.addFamily(Bytes.toBytes(family));
			}
			getList.add(get);
		}
		return readRow(getList);
	}

	private HBaseResultSet[] readRow(List<Get> get) throws Exception {
		long starttime = System.currentTimeMillis();
		try {
			Table table = getTable(properties, tableName);
			Result[] result = table.get(get);
			table.close();
			List<HBaseResultSet> resultSets = new ArrayList<HBaseResultSet>();
			// HBaseResultSet[] resultSets = new HBaseResultSet[result.length];
			for (int i = 0; i < result.length; i++) {
				if (result[i] != null && !result[i].isEmpty())
					resultSets.add(new HBaseResultSet(result[i]));
				// resultSets[i] = new HBaseResultSet(result[i]);
			}
			return resultSets.toArray(new HBaseResultSet[resultSets.size()]);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		} finally {
			executionTime = System.currentTimeMillis() - starttime;
		}
	}

	public void DeleteRow(String rowkey) throws Exception {
		long starttime = System.currentTimeMillis();
		if (tableName == null) {
			throw new Exception("Table name is null!");
		}
		Delete delete = new Delete(Bytes.toBytes(rowkey));
		try {
			Table table = getTable(properties, tableName);
			table.delete(delete);
			table.close();
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception(e);
		} finally {
		}
		executionTime = System.currentTimeMillis() - starttime;
	}

	// table scan
	public HBaseResultSet[] scan(HBaseColumn... columns) throws Exception {
		return scan(null, null, false, null, columns);
	}

	// table scan
	public HBaseResultSet[] scan(boolean reversed, HBaseColumn... columns) throws Exception {
		return scan(null, null, reversed, null, columns);
	}

	public HBaseResultSet[] scan(String startRowKey, String stopRowKey, HBaseColumn... columns) throws Exception {
		return scan(startRowKey, stopRowKey, false, null, columns);
	}

	public HBaseResultSet[] scan(String startRowKey, String stopRowKey, boolean reversed, Filter filter,
			HBaseColumn... columns) throws Exception {
		return scan(startRowKey, stopRowKey, reversed, filter, 0, null, columns);
	}

	public HBaseResultSet[] scan(String startRowKey, String stopRowKey, boolean reversed, Filter filter, int rows,
			HBaseResultComparator comparator, HBaseColumn... columns) throws Exception {

		Scan scan = new Scan();
		if (startRowKey != null)
			scan.setStartRow(Bytes.toBytes(startRowKey));

		if (stopRowKey != null)
			scan.setStopRow(Bytes.toBytes(stopRowKey));

		if (columns == null)
			scan.addFamily(HConstants.EMPTY_BYTE_ARRAY);
		else {
			for (HBaseColumn column : columns)
				scan.addColumn(Bytes.toBytes(column.getColumnFamily()), Bytes.toBytes(column.getColumnName()));
		}

		scan.setReversed(reversed);
		if (filter != null)
			scan.setFilter(filter);

		return scan(scan, rows, comparator);
	}

	public HBaseResultSet[] scan(String startRowKey, String stopRowKey, boolean reversed, Filter filter, String family)
			throws Exception {
		return scan(startRowKey, stopRowKey, reversed, filter, 0, null, family);
	}

	public HBaseResultSet[] scan(String startRowKey, String stopRowKey, boolean reversed, Filter filter, int rows,
			HBaseResultComparator comparator, String family) throws Exception {

		Scan scan = new Scan();
		if (startRowKey != null)
			scan.setStartRow(Bytes.toBytes(startRowKey));

		if (stopRowKey != null)
			scan.setStopRow(Bytes.toBytes(stopRowKey));

		scan.addFamily(Bytes.toBytes(family));

		scan.setReversed(reversed);
		if (filter != null)
			scan.setFilter(filter);

		return scan(scan, rows, comparator);
	}

	private HBaseResultSet[] scan(Scan scan, int rows, HBaseResultComparator comparator) throws Exception {
		long starttime = System.currentTimeMillis();
		Result[] results = null;
		try {
			Table table = getTable(properties, tableName);
			ResultScanner scanner = table.getScanner(scan);
			if (scanner == null)
				return new HBaseResultSet[0];
			if (rows < 1 || rows > Constants.MAX_HBASE_SCAN_SIZE)
				rows = Constants.MAX_HBASE_SCAN_SIZE;
			results = scanner.next(rows);
			if (results != null)
				this.rows = results.length;
			try {
				scanner.close();
				table.close();
			} catch (Exception e) {
			}
		} finally {
		}
		HBaseResultSet[] resultSets = new HBaseResultSet[results.length];
		for (int i = 0; i < results.length; i++) {
			resultSets[i] = new HBaseResultSet(results[i]);
		}

		if (comparator != null)
			Arrays.sort(resultSets, comparator);
		executionTime = System.currentTimeMillis() - starttime;
		return resultSets;
	}

	public long getExecutionTime() {
		return executionTime;
	}

	public int getRows() {
		return rows;
	}

	public void ensureTableExistence() {
		// TODO: implement logic to ensure table existence
	}

	public Connection getConnection(Properties prop) throws Exception {
		if (connection == null) {
			if (conf == null) {
				configure(prop);
			}
			try {
				synchronized (this) {
					if (connection == null)
						connection = ConnectionFactory.createConnection(conf);
				}

			} catch (IOException e) {
				e.printStackTrace();
				throw new Exception(
						"HBase IOException caught getting hConnection: see log for details. [" + e.getMessage() + "]");
			}
		}
		return connection;
	}

	private void configure(Properties prop) throws Exception {
		conf = HBaseConfiguration.create();
		String zookeepers = prop.getProperty(Constants.CONFIG_HBASE_ZOOKEEPER_QUORUM);
		if (zookeepers.length() < 1)
			throw new Exception("Missing HBase configuration [" + Constants.CONFIG_HBASE_ZOOKEEPER_QUORUM + "]");
		conf.set(Constants.CONFIG_HBASE_ZOOKEEPER_QUORUM, zookeepers);
		conf.set(Constants.CONFIG_HBASE_ZOOKEEPER_PORT,
				prop.getProperty(Constants.CONFIG_HBASE_ZOOKEEPER_PORT, Constants.DEFAULT_HBASE_ZOOKEEPER_PORT));
		conf.set(Constants.CONFIG_HBASE_ZOOKEEPER_ROOT,
				prop.getProperty(Constants.CONFIG_HBASE_ZOOKEEPER_ROOT, Constants.DEFAULT_HBASE_ZOOKEEPER_ROOT));
	}

	public Table getTable(Properties prop, String tableName) throws Exception {
		if (connection == null || connection.isClosed())
			connection = getConnection(prop);
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("Could not instantiate Table [" + e.getMessage() + "]");
		}
		return table;
	}

	public void cleanup() {
		try {
			write();
		} catch (Exception e) {
		}
		try {
			close();
		} catch (Exception e) {
		}
	}

	public void close() {
		try {
			connection.close();
		} catch (Exception e) {
		}
		try {
			tableMultiplexer.close();
		} catch (Exception e) {
		}
	}

	public boolean isActive() {
		boolean status = false;
		try {
			if (connection != null && !connection.isClosed())
				status = true;
		} catch (Exception e) {
		}
		return status;
	}

	public void regionStatus() throws Exception {
		getConnection(properties);
		Admin admin = connection.getAdmin();
		final ClusterStatus clusterStatus = admin.getClusterStatus();

		for (ServerName serverName : clusterStatus.getServers()) {
			final ServerLoad serverLoad = clusterStatus.getLoad(serverName);

			for (Map.Entry<byte[], RegionLoad> entry : serverLoad.getRegionsLoad().entrySet()) {
				final String region = Bytes.toString(entry.getKey());
				final RegionLoad regionLoad = entry.getValue();
				long storeFileSize = regionLoad.getStorefileSizeMB();
				System.out.println("region==" + region + "==store file size==" + storeFileSize);
				// other useful thing in regionLoad if you like
			}
		}
	}
}
