package com.database;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

public class DbManager {
	Connection connection;

	private Connection getConnection() {
		if (connection == null) {

			try {
				Configuration conf = HBaseConfiguration.create();
				connection = ConnectionFactory.createConnection(conf);
			} catch (Exception e) {

			}
		}
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
			HTableDescriptor tableName = new HTableDescriptor(TableName.valueOf("TweetInfo"));

			tableName.addFamily(new HColumnDescriptor("TweetDetails"));

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

}
