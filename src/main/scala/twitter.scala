import twitter4j.FilterQuery

class twitter(consumerKey: String, consumerSecret: String, accessToken: String, accessTokenSecret: String) {
  System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
  System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
  System.setProperty("twitter4j.oauth.accessToken", accessToken)
  System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)



}
