/**
 * 	TO  retrieve more than 100 tweets using the Twitter API.
 *
 * 	It is based upon the Application Authentication example, and therefore uses application
 * 	authentication.  It does not matter that much which type of authentication you use, although
 * 	it will effect your rate limits.

 * * 	
 */
//package us.mcguinness;
package com.twitterMgr;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import java.util.*;
import java.util.regex.Pattern;

import com.database.EnumRes;

import twitter4j.Query;
import twitter4j.Query.ResultType;
import twitter4j.QueryResult;
import twitter4j.RateLimitStatus;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;
import twitter4j.auth.OAuth2Token;
import twitter4j.conf.ConfigurationBuilder;

public class TweetFetcher {
	// Set this to your actual CONSUMER KEY and SECRET for your application as
	// given to you by dev.twitter.com
	protected String CONSUMER_KEY;
	protected String CONSUMER_SECRET;

	// How many tweets to retrieve in every call to Twitter. 100 is the maximum
	// allowed in the API
	protected int TWEETS_PER_QUERY;

	// This contrtextols how many queries, maximum, we will make of Twitter before
	// cutting off the results.
	// You will retrieve up to MAX_QUERIES*TWEETS_PER_QUERY tweets.
	//
	// If you set MAX_QUERIES high enough (e.g., over 450), you will undoubtedly
	// hit your rate limits
	// and you an see the program sleep until the rate limits reset
	protected int MAX_QUERIES;

	// What we want to search for in this program. #DeMonetisation always
	// returns as many results as you could
	// ever want, so it's safe to assume we'll get multiple pages back...
	protected String SEARCH_TERM;
	
		

	public TweetFetcher(String SearchTerm) {
		CONSUMER_KEY = "xz81qbMNskTmNvEkQjiIlzYzk";
		CONSUMER_SECRET = "KF0p7MNi8UQLkAv6seMI83ATocYW1ejjUyLtJN3r2sBzT0Uc9J";
		TWEETS_PER_QUERY = 50;
		MAX_QUERIES = 3;
		SEARCH_TERM = SearchTerm;
	}



	/**
	 * Replace newlines and tabs in text with escaped versions to making
	 * printing cleaner
	 *
	 * @param text
	 *            The text of a tweet, sometimes with embedded newlines and tabs and many more
	 * @return The text passed in, but with the newlines and tabs replaced
	 */
	public String cleanText(String text) {
		text = text.replaceAll("((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)", "");
		text = text.replaceAll("[^a-zA-Z0-9 ]+", "");
		text = text.replace("RT","");
		text = text.replace(",", " ");
		text = text.replace("\n", "\\n");
		text = text.replace("\t", "\\t");
	
		
		

		return text;
	}


	
	
	/**
	 * Retrieve the "bearer" token from Twitter in order to make
	 * application-authenticated calls.
	 *
	 * This is the first step in doing application authentication, as described
	 * in Twitter's documentation at
	 * https://dev.twitter.com/docs/auth/application-only-auth
	 *
	 * Note that if there's an error in this process, we just print a message
	 * and quit.
	 *
	 * @return The oAuth2 bearer token
	 */
	public OAuth2Token getOAuth2Token() {
		OAuth2Token token = null;
		ConfigurationBuilder cb;

		cb = new ConfigurationBuilder();
		cb.setApplicationOnlyAuthEnabled(true);

		cb.setOAuthConsumerKey(CONSUMER_KEY).setOAuthConsumerSecret(CONSUMER_SECRET);

		try {
			token = new TwitterFactory(cb.build()).getInstance().getOAuth2Token();
		} catch (Exception e) {
			System.out.println("Could not get OAuth2 token");
			e.printStackTrace();
			System.exit(0);
		}

		return token;
	}

	
	/**
	 * Get a fully application-authenticated Twitter object useful for making
	 * subsequent calls.
	 *
	 * @return Twitter4J Twitter object that's ready for API calls
	 */
	public Twitter getTwitter() {
		OAuth2Token token;

		// First step, get a "bearer" token that can be used for our requests
		token = getOAuth2Token();

		// Now, configure our new Twitter object to use application
		// authentication and provide it with
		// our CONSUMER key and secret and the bearer token we got back from
		// Twitter
		ConfigurationBuilder cb = new ConfigurationBuilder();

		cb.setApplicationOnlyAuthEnabled(true);

		cb.setOAuthConsumerKey(CONSUMER_KEY);
		cb.setOAuthConsumerSecret(CONSUMER_SECRET);

		cb.setOAuth2TokenType(token.getTokenType());
		cb.setOAuth2AccessToken(token.getAccessToken());

		// And create the Twitter object!
		return new TwitterFactory(cb.build()).getInstance();

	}

	
	public ArrayList<HashMap<String, String>> getTweetDetailsFromCSV(){
		ArrayList<HashMap<String, String>> tweetList = new ArrayList<HashMap<String, String>>();
		
		try{
			CsvReader tweets = new CsvReader(EnumRes.CLASSIFIEDTWEETSCSV.getValue());
			
			tweets.readHeaders();

			while (tweets.readRecord())
			{
				
				HashMap<String, String> tweetDict = new HashMap<String, String>();
				tweetDict.put(EnumRes.TWEETID.getValue(), tweets.get(EnumRes.TWEETID.getValue()));
				tweetDict.put(EnumRes.TWEETTOPIC.getValue(), tweets.get(EnumRes.TWEETTOPIC.getValue()));
				tweetDict.put(EnumRes.TWEETDATE.getValue(), tweets.get(EnumRes.TWEETDATE.getValue()));
				tweetDict.put(EnumRes.TWEETSCORE.getValue(), tweets.get(EnumRes.TWEETSCORE.getValue()));
				tweetDict.put(EnumRes.TWEETDESC.getValue(), tweets.get(EnumRes.TWEETDESC.getValue()));
				tweetList.add(tweetDict);
			
				
				
				
				// perform program logic here
				//System.out.println(productID + ":" + productName);
			}
			
		}catch(Exception e){
			
		}
		return tweetList;
	}
	
	
	public void runPythonClassifier() {
		// TODO Auto-generated method stub
		String prg = "import sys";
	       String s = null;
		
		try {
			//BufferedWriter out = new BufferedWriter(new FileWriter("/home/jagadeesh/PycharmProjects/TweetAnalyzer/a.py"));
			//out.write(prg);
			//out.close();
			Process p = Runtime.getRuntime().exec("python /home/jagadeesh/PycharmProjects/TweetAnalyzer/Test.py");
			   BufferedReader stdInput = new BufferedReader(new 
		                 InputStreamReader(p.getInputStream()));

		            BufferedReader stdError = new BufferedReader(new 
		                 InputStreamReader(p.getErrorStream()));

		         /*true  if(stdError!=null){
		        	   return -1;
		           }*/
		        	 
		           
		            
		            // read the output from the command
	            System.out.println("Here is the standard output of the command:\n");
		            while ((s = stdInput.readLine()) != null) {
		                System.out.println(s);
         }
//		            
//		            // read any errors from the attempted command
		            System.out.println("Here is the standard error of the command (if any):\n");
		            while ((s = stdError.readLine()) != null) {
		                System.out.println(s);
	            }
		           
		            
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		//return 0;
		
	}
	
	public ArrayList<HashMap<String, String>> getTweetList() {

		// We're curious how many tweets, in total, we've retrieved. Note that
		// TWEETS_PER_QUERY is an upper limit,
		// but Twitter can and often will retrieve far fewer tweets
		int totalTweets = 0;

		// This variable is the key to our retrieving multiple blocks of tweets.
		// In each batch of tweets we retrieve,
		// we use this variable to remember the LOWEST tweet ID. Tweet IDs are
		// (java) longs, and they are roughly
		// sequential over time. Without setting the MaxId in the query, Twitter
		// will always retrieve the most
		// recent tweets. Thus, to retrieve a second (or third or ...) batch of
		// Tweets, we need to set the Max Id
		// in the query to be one less than the lowest Tweet ID we've seen
		// already. This allows us to page backwards
		// through time to retrieve additional blocks of tweets
		long maxID = -1;

		Twitter twitter = getTwitter();
		ArrayList<HashMap<String, String>> tweetList = new ArrayList<HashMap<String, String>>();
		// Now do a simple search to show that the tokens work
		try {
			// There are limits on how fast you can make API calls to Twitter,
			// and if you have hit your limit
			// Thus, the proper thing to do is always check your limits BEFORE
			// making a call, and if you have
			// hit your limits sleeping until you are allowed to make calls
			// again.
			//
			// Every time you call the Twitter API, it tells you how many calls
			// you have left, so you don't have
			// to ask about the next call. But before the first call, we need to
			// find out whether we're already
			// at our limit.

			// This returns all the various rate limits in effect for us with
			// the Twitter API
			Map<String, RateLimitStatus> rateLimitStatus = twitter.getRateLimitStatus("search");

			// This finds the rate limit specifically for doing the search API
			// call we use in this program
			RateLimitStatus searchTweetsRateLimit = rateLimitStatus.get("/search/tweets");

			// Always nice to see these things when debugging code...
			// System.out.printf("You have %d calls remaining out of %d, Limit
			// resets in %d seconds\n",
			// searchTweetsRateLimit.getRemaining(),
			// searchTweetsRateLimit.getLimit(),
			// searchTweetsRateLimit.getSecondsUntilReset());

			// This will hold the fetched tweets which will be read by python ML
		/*	PrintWriter pw = new PrintWriter(
					new File("/home/jagadeesh/PycharmProjects/TweetAnalyzer/TempDatafiles/RawTweetsTemp.csv"));
			*/
			String outputFile="/home/jagadeesh/PycharmProjects/TweetAnalyzer/TempDatafiles/RawTweetsTemp.csv";
			CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, false), ',');
		
			csvOutput.write(EnumRes.TWEETID.getValue());
			csvOutput.write(EnumRes.TWEETTOPIC.getValue());
			csvOutput.write(EnumRes.TWEETDATE.getValue());
			csvOutput.write(EnumRes.TWEETDESC.getValue());
			csvOutput.endRecord();
			
			

			// This is the loop that retrieve multiple blocks of tweets from
			// Twitter
			for (int queryNumber = 0; queryNumber < MAX_QUERIES; queryNumber++) {
				// System.out.printf("\n\n!!! Starting loop %d\n\n",
				// queryNumber);

				// Do we need to delay because we've already hit our rate
				// limits?
				if (searchTweetsRateLimit.getRemaining() == 0) {
					// Yes we do, unfortunately ...
					System.out.printf("!!! Sleeping for %d seconds due to rate limits\n",
							searchTweetsRateLimit.getSecondsUntilReset());

					
					Thread.sleep((searchTweetsRateLimit.getSecondsUntilReset() + 2) * 1000l);
				}

				Query q = new Query("#"+SEARCH_TERM); // Search for tweets that
													// contains this term
				q.setCount(TWEETS_PER_QUERY); // How many tweets, max, to
												// retrieve

				q.resultType(ResultType.recent); // Get all tweets
				q.setLang("en"); // English language tweets, please

				// If maxID is -1, then this is our first call and we do not
				// want to tell Twitter what the maximum
				// tweet id is we want to retrieve. But if it is not -1, then it
				// represents the lowest tweet ID
				// we've seen, so we want to start at it-1 (if we start at
				// maxID, we would see the lowest tweet
				// a second time...
				if (maxID != -1) {
					q.setMaxId(maxID - 1);
				}

				// This actually does the search on Twitter and makes the call
				// across the network
				QueryResult r = twitter.search(q);

				// If there are NO tweets in the result set, it is Twitter's way
				// of telling us that there are no
				// more tweets to be retrieved. Remember that Twitter's search
				// index only contains about a week's
				// worth of tweets, and uncommon search terms can run out of
				// week before they run out of tweets
				if (r.getTweets().size() == 0) {
					break; // Nothing? We must be done
				}

				// loop through all the tweets and process them.
				for (Status s : r.getTweets()) {
					// Increment our count of tweets retrieved
					totalTweets++;

					// Keep track of the lowest tweet ID. If you do not do this,
					// you cannot retrieve multiple
					// blocks of tweets...
					if (maxID == -1 || s.getId() < maxID) {
						maxID = s.getId();
					}

					// Do something with the tweet....
					// System.out.printf("At %s, @%-20s said: %s\n",
					// s.getCreatedAt().toString(),
					// s.getUser().getScreenName(),
					// cleanText(s.getText()));

				/*	HashMap<String, String> tweetDict = new HashMap<String, String>();
					tweetDict.put(EnumRes.TWEETID.getValue(), String.valueOf(s.getId()));
					tweetDict.put(EnumRes.TWEETTOPIC.getValue(), SEARCH_TERM);
					tweetDict.put(EnumRes.TWEETDESC.getValue(), cleanText(s.getText()));
					tweetDict.put(EnumRes.TWEETDATE.getValue(), s.getCreatedAt().toString());

					tweetList.add(tweetDict);*/

			
					
					
					csvOutput.write(String.valueOf(s.getId()));
					csvOutput.write(SEARCH_TERM);
					csvOutput.write(s.getCreatedAt().toString());
					csvOutput.write(cleanText(s.getText()));
					csvOutput.endRecord();

				}

				// As part of what gets returned from Twitter when we make the
				// search API call, we get an updated
				// status on rate limits. We save this now so at the top of the
				// loop we can decide whether we need
				// to sleep or not before making the next call.
				searchTweetsRateLimit = r.getRateLimitStatus();

			}
			// copy the content of stringbuilder to file
			//pw.write(tweets.toString());
			//pw.close();
			csvOutput.close();
		} catch (Exception e) {
			// Catch all -- you're going to read the stack trace and figure out
			// what needs to be done to fix it
			System.out.println("Something went wrong");

			e.printStackTrace();

		}

		System.out.printf("\n\nA total of %d tweets retrieved\n", totalTweets);
		runPythonClassifier();
		tweetList = getTweetDetailsFromCSV();
		/*if(runPythonClassifier()!=-1){
			tweetList = getTweetDetailsFromCSV();
		}*/
		return tweetList;

	}

	
	
	

}
