package com.database;

import com.csvreader.CsvReader;
import com.twitterMgr.TweetFetcher;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.commons.configuration.SystemConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.util.Bytes;

public class TweetManager {

	private static byte[] TWEETDETAILS_CF = Bytes.toBytes(EnumRes.TWEETDETAILS.getValue());
	private static byte[] TWEETTOPICS_CF = Bytes.toBytes(EnumRes.TWEETTOPICS_CF.getValue());
	private static byte[] TWEETTOPICNAME_COLUMN = Bytes.toBytes(EnumRes.TWEETTOPICNAME.getValue());
	private static byte[] TWEETID_COLUMN = Bytes.toBytes(EnumRes.TWEETID.getValue());
	private static byte[] TWEETDESC_COLUMN = Bytes.toBytes(EnumRes.TWEETDESC.getValue());
	private static byte[] TWEETTOPIC_COLUMN = Bytes.toBytes(EnumRes.TWEETTOPIC.getValue());
	private static byte[] TWEETDATE_COLUMN = Bytes.toBytes(EnumRes.TWEETDATE.getValue());
	private static byte[] TWEETSCORE_COLUMN = Bytes.toBytes(EnumRes.TWEETSCORE.getValue());

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		long startTime = System.currentTimeMillis();

		TweetManager twtMgr = new TweetManager();

		DbManager dbMgr = new DbManager();
		//dbMgr.createTable();

		String topicName ="#arrow";
		//TweetFetcher tfr = new TweetFetcher(topicName);

		//twtMgr.insertTweets(tfr.getTweetList(), dbMgr);
		//twtMgr.insertTweetTopic(topicName, dbMgr);

		List<HashMap<String, String>> tweetDataList = twtMgr.getTweetDataList("#trump", dbMgr);
		List<String> tweetTopicList = twtMgr.getTweetTopicList(dbMgr);
		long estimatedTime = System.currentTimeMillis() - startTime;
		double elapsedSeconds = estimatedTime / 1000.0;
		System.out.println("All ops completed and time spent was :" + elapsedSeconds);

	}

	public void insertTweets(ArrayList<HashMap<String, String>> tweetList, DbManager dbmgr) {

		HashMap<String, String> tblMetaData = new HashMap<String, String>();
		tblMetaData.put(EnumRes.TABLENAME.getValue(), EnumRes.TWEETINFO.getValue());
		tblMetaData.put(EnumRes.COLUMNFAMILYNAME.getValue(), EnumRes.TWEETDETAILS.getValue());
		
		List<Put> tweetPutList = new ArrayList<Put>();
		for (HashMap<String, String> tweetDict : tweetList) {

			Put tweetPut = new Put(Bytes.toBytes(tweetDict.get(EnumRes.TWEETID.getValue())));
			tweetPut.addColumn(TWEETDETAILS_CF, TWEETTOPIC_COLUMN,
					Bytes.toBytes(tweetDict.get(EnumRes.TWEETTOPIC.getValue())));
			tweetPut.addColumn(TWEETDETAILS_CF, TWEETDATE_COLUMN,
					Bytes.toBytes(tweetDict.get(EnumRes.TWEETDATE.getValue())));
			tweetPut.addColumn(TWEETDETAILS_CF, TWEETSCORE_COLUMN,
					Bytes.toBytes(tweetDict.get(EnumRes.TWEETSCORE.getValue())));
			tweetPut.addColumn(TWEETDETAILS_CF, TWEETDESC_COLUMN,
					Bytes.toBytes(tweetDict.get(EnumRes.TWEETDESC.getValue())));

			tweetPutList.add(tweetPut);
		}
		dbmgr.insertData(tblMetaData, tweetPutList);
	}

	public void insertTweetTopic(String topicName, DbManager dbmgr) {

		HashMap<String, String> tblMetaData = new HashMap<String, String>();
		tblMetaData.put(EnumRes.TABLENAME.getValue(), EnumRes.TWEETINFO.getValue());
		tblMetaData.put(EnumRes.COLUMNFAMILYNAME.getValue(), EnumRes.TWEETTOPICS_CF.getValue());

		List<Put> tweetPutList = new ArrayList<Put>();
		UUID uuid = UUID.randomUUID();

		Put tweetPut = new Put(Bytes.toBytes(uuid.toString()));
		tweetPut.addColumn(TWEETTOPICS_CF, TWEETTOPICNAME_COLUMN, Bytes.toBytes(topicName));

		tweetPutList.add(tweetPut);

		dbmgr.insertData(tblMetaData, tweetPutList);
	}

	public List<HashMap<String, String>> getTweetDataList(String topicName, DbManager dbmgr) {

		HashMap<String, String> queryMetaData = new HashMap<String, String>();
		queryMetaData.put(EnumRes.TABLENAME.getValue(), EnumRes.TWEETINFO.getValue());
		queryMetaData.put(EnumRes.COLUMNFAMILYNAME.getValue(), EnumRes.TWEETDETAILS.getValue());
		queryMetaData.put(EnumRes.COLUMNNAME.getValue(), EnumRes.TWEETTOPIC.getValue());
		queryMetaData.put(EnumRes.COLUMNVALUE.getValue(), topicName);
		queryMetaData.put(EnumRes.OPERATOR.getValue(), EnumRes.EQUAL.getValue());

		ResultScanner scanResult = dbmgr.getTableDataFromColumnValue(queryMetaData);
		return getTweetDetailsListFromScan(scanResult);

	}

	public List<HashMap<String, String>> getTweetDetailsListFromScan(ResultScanner scanResult) {

		HashMap<String, String> queryDict = new HashMap<String, String>();
		List<HashMap<String, String>> queryResultList = new ArrayList<HashMap<String, String>>();
		String oldRow = null;

		for (Result res : scanResult) {
			for (Cell cell : res.listCells()) {
				String newRow = new String(CellUtil.cloneRow(cell));
				// String family = new String(CellUtil.cloneFamily(cell));
				if (!newRow.equals(oldRow)) {
					// System.out.println(newRow + " ");
					queryResultList.add(queryDict);
					queryDict = new HashMap<String, String>();
					queryDict.put(EnumRes.TWEETID.getValue(), newRow);
				}
				String column = new String(CellUtil.cloneQualifier(cell));
				String value = new String(CellUtil.cloneValue(cell));
				queryDict.put(column, value);

				oldRow = newRow;
			}
		}

		// Removing first inserted row of list since it is null
		queryResultList.remove(0);

		System.out.println("printing size of list");
		System.out.println(queryResultList.size());

		for (HashMap<String, String> queryResDict : queryResultList) {
			System.out.println("----------------");
			System.out.println(EnumRes.TWEETID.getValue() + ":" + queryResDict.get(EnumRes.TWEETID.getValue()));
			System.out.println(EnumRes.TWEETDATE.getValue() + ":" + queryResDict.get(EnumRes.TWEETDATE.getValue()));
			System.out.println(EnumRes.TWEETTOPIC.getValue() + ":" + queryResDict.get(EnumRes.TWEETTOPIC.getValue()));
			System.out.println(EnumRes.TWEETSCORE.getValue() + ":" + queryResDict.get(EnumRes.TWEETSCORE.getValue()));
			System.out.println(EnumRes.TWEETDESC.getValue() + ":" + queryResDict.get(EnumRes.TWEETDESC.getValue()));
		}

		System.out.println("printing size of list");
		System.out.println(queryResultList.size());

		return queryResultList;

	}

	public List<String> getTweetTopicListFromScan(ResultScanner scanResult) {

		String tweetTopicName;
		List<String> queryResultList = new ArrayList<String>();

		for (Result res : scanResult) {
			for (Cell cell : res.listCells()) {

				tweetTopicName = new String(CellUtil.cloneValue(cell));
				queryResultList.add(tweetTopicName);
			}
		}

		// Removing first inserted row of list since it is null
		// queryResultList.remove(0);

		System.out.println("printing size of list");
		System.out.println(queryResultList.size());

		for (String topicName : queryResultList) {
			System.out.println("----------------");
			System.out.println(topicName);

		}

		return queryResultList;

	}

	public List<String> getTweetTopicList(DbManager dbmgr) {

		HashMap<String, String> queryMetaData = new HashMap<String, String>();
		queryMetaData.put(EnumRes.TABLENAME.getValue(), EnumRes.TWEETINFO.getValue());
		queryMetaData.put(EnumRes.COLUMNFAMILYNAME.getValue(), EnumRes.TWEETTOPICS_CF.getValue());
		queryMetaData.put(EnumRes.COLUMNNAME.getValue(), EnumRes.TWEETTOPICNAME.getValue());

		ResultScanner scanResult = dbmgr.getTableDataFromColumnName(queryMetaData);
		List<String> queryResultList = getTweetTopicListFromScan(scanResult);

		return queryResultList;

	}

}
