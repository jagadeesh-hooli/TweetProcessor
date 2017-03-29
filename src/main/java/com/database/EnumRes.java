package com.database;

public enum EnumRes {
	TWEETINFO("TweetInfo"),
	TWEETDETAILS("TweetDetails"),
	TWEETTOPICS_CF("TweetTopics"),
	TWEETTOPICNAME("TopicName"),
	TWEETID("Id"),
	TWEETDESC("Desc"),
	TWEETTOPIC("Topic"),
	TWEETDATE("DateOfTweet"),
	TABLENAME("TableName"),
	COLUMNFAMILYNAME("ColumnFamilyName"),
	COLUMNNAME("ColumnName"),
	OPERATOR("Operator"),
	EQUAL("Equal"),
	COLUMNVALUE("ColumnValue"),
	TWEETSCORE("SentiScore"),
	CLASSIFIEDTWEETSCSV("/home/jagadeesh/PycharmProjects/TweetAnalyzer/TempDatafiles/ClassifiedTweetsTemp.csv"),
	PYTHONCLASSIFIER("/home/jagadeesh/PycharmProjects/TweetAnalyzer/Test.py");
	

	private String value;

	private EnumRes(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}

}
