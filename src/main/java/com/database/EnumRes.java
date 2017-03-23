package com.database;

public enum EnumRes {
	TWEETINFO("TweetInfo"),
	TWEETDETAILS("TweetDetails"),
	TWEETID("Id"),
	TWEETDESC("Desc"),
	TWEETTOPIC("Topic"),
	TWEETDATE("DateOfTweet"),
	TABLENAME("TableName"),
	COLUMNFAMILYNAME("ColumnFamilyName"),
	COLUMNNAME("ColumnName"),
	OPERATOR("Operator"),
	EQUAL("Equal"),
	COLUMNVALUE("ColumnValue");
	

	private String value;

	private EnumRes(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

}
