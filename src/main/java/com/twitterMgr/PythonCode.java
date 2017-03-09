package com.twitterMgr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
public class PythonCode {

	public static void main(String[] args) {
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

		            // read the output from the command
		            System.out.println("Here is the standard output of the command:\n");
		            while ((s = stdInput.readLine()) != null) {
		                System.out.println(s);
		            }
		            
		            // read any errors from the attempted command
		            System.out.println("Here is the standard error of the command (if any):\n");
		            while ((s = stdError.readLine()) != null) {
		                System.out.println(s);
		            }
		            
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

}

