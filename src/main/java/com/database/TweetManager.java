package com.database;

import com.twitterMgr.TweetFetcher;
import java.util.*;

import org.apache.commons.configuration.SystemConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

public class TweetManager {

    private static byte[] TWEETDETAILS_CF = Bytes.toBytes(EnumRes.TWEETDETAILS.getValue());
    private static byte[] TWEETID_COLUMN = Bytes.toBytes(EnumRes.TWEETID.getValue());
    private static byte[] TWEETDESC_COLUMN = Bytes.toBytes(EnumRes.TWEETDESC.getValue());
    private static byte[] TWEETTOPIC_COLUMN = Bytes.toBytes(EnumRes.TWEETTOPIC.getValue());
    private static byte[] TWEETDATE_COLUMN = Bytes.toBytes(EnumRes.TWEETDATE.getValue());
    
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		DbManager dbMgr = new DbManager();
		TweetManager twtMgr = new TweetManager();
		//dbmgr.createTable();
		TweetFetcher tfr = new TweetFetcher("#DeMonetisation");
		//twtMgr.insertTweets(tfr.getTweetList(),dbMgr);
		
		twtMgr.getTweetData("#DeMonetisation",dbMgr);

	}
	
	public void insertTweets(ArrayList<HashMap<String,String>> tweetList,DbManager dbmgr){
		
		HashMap<String,String> tblMetaData= new HashMap<String,String>();
		tblMetaData.put(EnumRes.TABLENAME.getValue(), EnumRes.TWEETINFO.getValue());
		tblMetaData.put(EnumRes.COLUMNFAMILYNAME.getValue(), EnumRes.TWEETDETAILS.getValue());
		
		List<Put> tweetPutList = new ArrayList<Put>();
		for(HashMap<String,String> tweetDict:tweetList){
			
			Put tweetPut = new Put(Bytes.toBytes(tweetDict.get(EnumRes.TWEETID.getValue())));
			tweetPut.addColumn(TWEETDETAILS_CF , TWEETTOPIC_COLUMN , Bytes.toBytes(tweetDict.get(EnumRes.TWEETTOPIC.getValue())));
			tweetPut.addColumn(TWEETDETAILS_CF , TWEETDATE_COLUMN  , Bytes.toBytes(tweetDict.get(EnumRes.TWEETDATE.getValue())));
			tweetPut.addColumn(TWEETDETAILS_CF , TWEETDESC_COLUMN  , Bytes.toBytes(tweetDict.get(EnumRes.TWEETDESC.getValue())));
		
			tweetPutList.add(tweetPut);
		}
		dbmgr.insertData(tblMetaData,tweetPutList);
	}
	
	public void getTweetData(String topicName,DbManager dbmgr){
		
		HashMap<String,String> queryMetaData= new HashMap<String,String>();
		queryMetaData.put(EnumRes.TABLENAME.getValue(), EnumRes.TWEETINFO.getValue());
		queryMetaData.put(EnumRes.COLUMNFAMILYNAME.getValue(), EnumRes.TWEETDETAILS.getValue());
		queryMetaData.put(EnumRes.COLUMNNAME.getValue(), EnumRes.TWEETTOPIC.getValue());
		queryMetaData.put(EnumRes.COLUMNVALUE.getValue(),topicName);
		queryMetaData.put(EnumRes.OPERATOR.getValue(),EnumRes.EQUAL.getValue());
		
		dbmgr.getTableDataFromColumnValue(queryMetaData);
	}
	
	
	

}
