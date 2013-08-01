/*******************************************************************************
 * Copyright (c) 2013, Salesforce.com, Inc.
 * All rights reserved.
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *     Redistributions in binary form must reproduce the above copyright notice,
 *     this list of conditions and the following disclaimer in the documentation
 *     and/or other materials provided with the distribution.
 *     Neither the name of Salesforce.com nor the names of its contributors may 
 *     be used to endorse or promote products derived from this software without 
 *     specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE 
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR 
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER 
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, 
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE 
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 ******************************************************************************/
package com.salesforce.jirachi.ingest;

import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.atlassian.jira.rest.client.api.*;
import com.atlassian.jira.rest.client.api.domain.*;
import com.atlassian.jira.rest.client.auth.*;
import com.atlassian.jira.rest.client.internal.async.AsynchronousJiraRestClientFactory;
import com.atlassian.util.concurrent.Promise;
import com.google.common.collect.Lists;

/**
 * Ingest a bunch of issues from Jira using the REST API
 */
public class JiraFetcher {
  
  private static String baseUrl = System.getProperty("jirachi.baseUrl");
  private static String restQuery = System.getProperty("jirachi.restQuery");

  public static class JiraSet{
    JiraSet(Map<String,Issue> issues, Map<String,List<Comment>> comments, Map<String,List<Attachment>> attachments) {
      this.issues = issues;
      this.comments = comments;
      this.attachments = attachments;
    }
    public Map<String,Issue> issues;
    public Map<String,List<Comment>> comments;
    public Map<String,List<Attachment>> attachments;
  }

	public static void fetchAndPersist() throws Exception{
	  JiraRestClientFactory j = new AsynchronousJiraRestClientFactory();
	  JiraRestClient c = j.create(new URI(baseUrl), new AnonymousAuthenticationHandler());
	  SearchRestClient s = c.getSearchClient();
    IssueRestClient irc = c.getIssueClient();
	  String jql = restQuery;
	  // query once to get the size
    Promise<SearchResult> sizeQuery = s.searchJql(jql,1,0);
    SearchResult sizeQueryResult = sizeQuery.get();
    int totalSize = sizeQueryResult.getTotal();
    System.out.println("Total issues in query: " + totalSize);
    
    // now query in batches
    int totalResults = 0;
	  int BATCH_SIZE = 50;
	  while (totalResults < totalSize) {
	    
	    Map<String,Issue> issues = new HashMap<String,Issue>(totalSize);
	    Map<String,List<Comment>> commentsByIssue = new HashMap<String,List<Comment>>();
	    Map<String,List<Attachment>> attachmentsByIssue = new HashMap<String,List<Attachment>>();

	    Promise<SearchResult> batch = s.searchJql(jql, BATCH_SIZE, totalResults);
	    SearchResult batchResult = batch.claim();
	    
	    int totalCommentsInThisBatch = 0;
	    int totalInThisBatch = 0;
 	    for (Issue i : batchResult.getIssues()) {
	      issues.put(i.getKey(), i);
	      commentsByIssue.put(i.getKey(), getComments(i, irc));
	      attachmentsByIssue.put(i.getKey(), getAttachments(i,irc));
	      totalResults++;
	      totalInThisBatch++;
	      totalCommentsInThisBatch += commentsByIssue.get(i.getKey()).size();
	    }
 	    System.out.print("Persisting batch of " + totalInThisBatch + " issues, with " + totalCommentsInThisBatch + " comments.");
 	    PhoenixJiraPersister.persist(new JiraSet(issues, commentsByIssue, attachmentsByIssue));
	  }
	  c.destroy();
	}

  private static List<Attachment> getAttachments(Issue i, IssueRestClient irc) throws Exception {
    // This doesn't seem to work for attachments, temporarily commented out.
    // Asked for help here: https://answers.atlassian.com/questions/175550/getting-issue-attachments-in-jrjc#
    //Issue is = irc.getIssue(i.getKey()).claim();
    //if (is == null || is.getAttachments() == null) return Lists.newArrayList();
    //return Lists.newArrayList(is.getAttachments());
    return Lists.newArrayList();
  }

  private static List<Comment> getComments(Issue i, IssueRestClient c) throws Exception {
    Issue is = c.getIssue(i.getKey()).claim();
    return Lists.newArrayList(is.getComments());
  }

}
