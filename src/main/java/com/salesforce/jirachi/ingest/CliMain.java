/*
 * Copyright, 1999-2012, SALESFORCE.com 
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.jirachi.ingest;

import java.util.Map;

import com.atlassian.jira.rest.client.api.domain.Issue;
import com.salesforce.jirachi.ingest.JiraFetcher.JiraSet;

/**
 * Runs the jirachi import process
 */
public class CliMain {
    public static void main(String[] args) throws Exception {
      JiraFetcher.fetchAndPersist();
    }
    
}
