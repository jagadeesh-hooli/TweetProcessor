# TweetProcessor
* This is middleware Java Maven project. 
* Establishes OAUth connection with Twitter and retrieves tweets from Twitter and dumps into csv file in a shared folder.
* Delegates TweetAnalyzer project(Please check for the repository with the same name in my account) to classify the tweets using its machine learning model built in Python.
* Stores sentimentally classified tweets in HBase ( Haddop file system)
* HBase and Hadoop are configured in Pseudo-Distributed mode simulating two cluster nodes.
* Supports CRUD operations on tweets related information using HBase java apis.
* For a complete details of the project and its synchronization with other projects please <a href="https://drive.google.com/file/d/0B54zuGD6R78ZY28wbkRMZV9vdHc/view">refer here</a>
