package com.salesforce.jirachi.ingest;

import java.net.URI;

import com.atlassian.jira.rest.client.api.*;
import com.atlassian.jira.rest.client.api.domain.SearchResult;
import com.atlassian.jira.rest.client.auth.*;
import com.atlassian.jira.rest.client.internal.async.AsynchronousJiraRestClientFactory;
import com.atlassian.util.concurrent.Promise;

/**
 * Ingest a bunch of issues from Jira using the REST API
 *
 */
public class JiraFetcher {

	public static void fetch() throws Exception{
    System.out.println("Hello world, I'm a Jira Fetcher.");
    
	  JiraRestClientFactory j = new AsynchronousJiraRestClientFactory();
	  JiraRestClient c = j.create(new URI("https://issues.apache.org/jira/"), new AnonymousAuthenticationHandler());
	  SearchRestClient s = c.getSearchClient();
	  String jql = "project%20%3D%20HBASE%20AND%20resolved%20>%3D%202012-05-23%20AND%20resolved%20<%3D%202013-05-24";
	  // query once to get the size
    Promise<SearchResult> sizeQuery = s.searchJql(jql);
    SearchResult sizeQueryResult = sizeQuery.get();
    int totalSize = sizeQueryResult.getTotal();
    System.out.println("Total issues: " + totalSize);
    
	}
}
