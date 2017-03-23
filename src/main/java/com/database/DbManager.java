package com.database;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.codehaus.jackson.map.ser.impl.SerializerCache.TypeKey;

public class DbManager {
	private Connection connection;

	DbManager() {
		try {
			Configuration conf = HBaseConfiguration.create();
			this.connection = ConnectionFactory.createConnection(conf);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private Connection getConnection() {

		return connection;

	}

	protected void closeConnection() {
		try {
			connection.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	protected void createTable() {

		Admin admin;
		try {
			connection = getConnection();
			admin = connection.getAdmin();
			HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf(EnumRes.TWEETINFO.getValue()));

			tableName.addFamily(new HColumnDescriptor(EnumRes.TWEETDETAILS.getValue()));

			if (!admin.tableExists(tableName.getTableName())) {
				System.out.print("Creating the TweetDetails table. ");

				admin.createTable(tableName);

				System.out.println("Done.");
			} else {
				System.out.println("Table already exists");
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	protected void insertData(HashMap<String, String> tblMetaData, List<Put> tblDataList) {
		connection = getConnection();
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tblMetaData.get(EnumRes.TABLENAME.getValue())));
			table.put(tblDataList);

		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("Insertion of data failed:" + e.getMessage());
		}

	}

	protected void getTableDataFromColumnValue(HashMap<String, String> queryMetaData) {
		connection = getConnection();
		Table table = null;
		ResultScanner scanResult = null;
		CompareOp operator = null;
		try {
			table = connection.getTable(TableName.valueOf(queryMetaData.get(EnumRes.TABLENAME.getValue())));
			if (queryMetaData.get(EnumRes.OPERATOR) == queryMetaData.get(EnumRes.EQUAL)) {
				operator = CompareOp.EQUAL;
			}

			SingleColumnValueFilter filter = new SingleColumnValueFilter(
					Bytes.toBytes(queryMetaData.get(EnumRes.COLUMNFAMILYNAME.getValue())),
					Bytes.toBytes(queryMetaData.get(EnumRes.COLUMNNAME.getValue())), operator,
					new BinaryComparator(Bytes.toBytes(queryMetaData.get(EnumRes.COLUMNVALUE.getValue()))));

			filter.setFilterIfMissing(true);

			Scan userScan = new Scan();
			userScan.setFilter(filter);

			scanResult = table.getScanner(userScan);

			String previousRowKey = null;
			HashMap<String, String> queryDict = new HashMap<String, String>();
			List<HashMap<String, String>> queryResultList = new ArrayList<HashMap<String, String>>();

			for (Result res : scanResult) {
				for (Cell cell : res.listCells()) {
					String row = new String(CellUtil.cloneRow(cell));
					String family = new String(CellUtil.cloneFamily(cell));
					String column = new String(CellUtil.cloneQualifier(cell));
					String value = new String(CellUtil.cloneValue(cell));

					System.out.println(row + " " + family + " " + column + " " + value);
				}
			}

		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("Insertion of data failed:" + e.getMessage());
		}

	}

}
