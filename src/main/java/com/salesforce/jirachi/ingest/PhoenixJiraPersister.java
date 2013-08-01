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

import java.sql.Connection;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import com.atlassian.jira.rest.client.api.NamedEntity;
import com.atlassian.jira.rest.client.api.domain.*;
import com.salesforce.jirachi.ingest.JiraFetcher.JiraSet;

/**
 * Dump a map of JIRA issues into Phoenix
 */
public class PhoenixJiraPersister  {
  
  public static void persist(JiraSet js) throws Exception {
    Connection conn = PhoenixUtils.getPhoenixConnection();
    try {
      createSchema(conn);
      // push the comments into the comment table
      for (Entry<String,List<Comment>> e : js.comments.entrySet()){
        insertComments(e.getKey(), e.getValue(), conn);
      }
      // push the attachments into the attachment table
      for (Entry<String,List<Attachment>> e : js.attachments.entrySet()){
        insertAttachments(e.getKey(), e.getValue(), conn);
      }
      // push the issues into the issue table
      for (Issue i : js.issues.values()){
        int numComments = js.comments.get(i.getKey()).size();
        int numAttachments = js.attachments.get(i.getKey()).size();
        insertIssue(i, numComments, numAttachments, conn);
      }
    } finally {
      conn.close();
    }
  }

  private static void insertAttachments(String key, List<Attachment> attachments, Connection conn) throws Exception {
    for (Attachment a : attachments){
      
      StringBuilder upsert = new StringBuilder("UPSERT INTO JIRA_ATTACHMENT (")
      .append("issue_key, ")
      .append("filename, ")
      .append("contentUri, ")
      .append("author, ")
      .append("mimeType, ")
      .append("size, ")
      .append("creationDate");
      
      upsert.append(") VALUES (")
          .append(getString(key) + ", ")
          .append(getString(a.getFilename()) + ", ")
          .append(getString(a.getContentUri()) + ", ")
          .append(getString(a.getAuthor()) + ", ")
          .append(getString(a.getMimeType()) + ", ")
          .append(a.getSize() + ", ")
          .append(getNullableDate(a.getCreationDate()) + ")");
      
      PhoenixUtils.execute(conn, upsert.toString());
    }
  }

  private static void insertComments(String key, List<Comment> comments, Connection conn) throws Exception {
    for (Comment c : comments){
      
      StringBuilder upsert = new StringBuilder("UPSERT INTO JIRA_COMMENT (")
      .append("comment_id, ")
      .append("issue_key, ")
      .append("author, ")
      .append("body, ")
      .append("creationDate");
      
      upsert.append(") VALUES (")
          .append(getLong(c.getId()) + ", ")
          .append(getString(key) + ", ")
          .append(getString(c.getAuthor()) + ", ")
          .append(getString(c.getBody()) + ", ")
          .append(getNullableDate(c.getCreationDate()) + ")");
      
      PhoenixUtils.execute(conn, upsert.toString());
    }
  }

  private static void insertIssue(Issue issue, int numComments, int numAttachments, Connection conn) throws Exception {
    StringBuilder upsert = new StringBuilder("UPSERT INTO JIRA_ISSUE (")
    .append("id, ")
    .append("issue_key, ")
    .append("summary, ")
    .append("description, ")
    .append("assignee, ")
    .append("reporter, ")
    .append("creationDate, ")
    .append("updateDate, ")
    .append("issueType, ")
    .append("priority, ")
    .append("resolution, ")
    .append("status, ")
    .append("watchers, ")
    .append("numAffectedVersions, ")
    .append("fixVersions, ")
    .append("numAttachments, ")
    .append("numChangelogs, ")
    .append("numComments, ")
    .append("numIssueLinks, ")
    .append("numLabels, ")
    .append("numSubtasks, ")
    .append("numWorkLogs");
    
    upsert.append(") VALUES (")
        .append(getLong(issue.getId()) + ", ")
        .append(getString(issue.getKey()) + ", ")
        .append(getString(issue.getSummary()) + ", ")
        .append(getString(issue.getDescription()) + ", ")
        .append(getString(issue.getAssignee()) + ", ")
        .append(getString(issue.getReporter()) + ", ")
        .append(getNullableDate(issue.getCreationDate()) + ", ")
        .append(getNullableDate(issue.getUpdateDate()) + ", ")
        .append(getString(issue.getIssueType()) + ", ")
        .append(getString(issue.getPriority()) + ", ")
        .append(getString(issue.getResolution()) + ", ")
        .append(getString(issue.getStatus()) + ", ")
        .append(getInteger(issue.getWatchers()) + ", ")
        .append(getCount(issue.getAffectedVersions()) + ", ")
        .append(getCount(issue.getFixVersions()) + ", ")
        .append(numAttachments + ", ")
        .append(getCount(issue.getChangelog()) + ", ")
        .append(numComments + ", ")
        .append(getCount(issue.getIssueLinks()) + ", ")
        .append(getCount(issue.getLabels()) + ", ")
        .append(getCount(issue.getSubtasks()) + ", ")
        .append(getCount(issue.getWorklogs()) + ")");
    
    PhoenixUtils.execute(conn, upsert.toString());
  }

  private static String getLong(Long l) {
    if (l == null) return "null";
    return l.toString();
  }

  @SuppressWarnings("rawtypes")
  private static String getCount(Iterable i) {
    if (i == null) return "null";
    int counter = 0;
    Iterator it = i.iterator();
    while (it.hasNext() && it.next() != null){
      counter++;
    }
    return String.valueOf(counter);
  }

  private static String getInteger(BasicWatchers watchers) {
    if (watchers != null)
      return String.valueOf(watchers.getNumWatchers());
    else
      return "null";
  }

  private static DateTimeFormatter f = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSS");
  
  private static String getNullableDate(DateTime d) {
    if(d == null) return "null"; 
    return "TO_DATE('" + d.toString(f) + "')";
  }

  private static String getString(Object thing) {
    if (thing == null) return null;
    if (thing.getClass().equals(User.class) ||
        thing.getClass().equals(BasicUser.class)){
      return "'" + ((BasicUser)thing).getDisplayName().replace("'", "''") + "'";
    } else if (thing.getClass().equals(BasicIssueType.class) ||
      thing.getClass().equals(BasicPriority.class) ||
      thing.getClass().equals(BasicResolution.class) ||
      thing.getClass().equals(BasicStatus.class)) {
      return "'" + ((NamedEntity)thing).getName() + "'";
    } else {
      return "'" + thing.toString().replace("'", "''").replace("\\", "\\\\").replace("\"","\\\"").replace('\n', ' ') + "'";
    }
  }
  
  private static void createSchema(Connection conn) throws Exception {
    createCommentTable(conn);
    createAttachmentTable(conn);
    createJiraTable(conn);
  }


  private static void createJiraTable(Connection conn) throws Exception {
    String SQL = "CREATE TABLE JIRA_ISSUE (id bigint NOT NULL, " +
        "issue_key varchar(11) NOT NULL, " +
        "summary varchar NOT NULL, " +
        "description varchar NULL, " +
        "assignee varchar NULL, " +
        "reporter varchar NULL, " +
        "creationDate timestamp NULL, " +
        "updateDate timestamp NULL, " +
        "issueType varchar NULL, " +
        "priority varchar NULL, " +
        "resolution varchar NULL, " +
        "status varchar NULL, " +
        "watchers integer NULL, " +
        "numAffectedVersions integer NULL, " +
        "fixVersions integer NULL, " +
        "numAttachments integer NULL, " +
        "numChangelogs integer NULL, " +
        "numComments integer NULL, " +
        "numIssueLinks integer NULL, " +
        "numLabels integer NULL, " +
        "numSubtasks integer NULL, " +
        "numWorkLogs integer NULL, " +
        createColumns(JiraTagger.getTagNames()) + 
        "CONSTRAINT pk primary key (id))";
    PhoenixUtils.executeNoTableExistsThrow(conn, SQL);
  }
  
  /**
   * Using all the tag names from the JiraTagger, create the right syntax for adding extra columns to the table
   */
  private static String createColumns(Collection<String> tagNames) {
    StringBuilder s = new StringBuilder();
    for (String tagName : tagNames){
      s.append(tagName).append(" BOOLEAN, ");
    }
    return s.toString().substring(0, s.length() - 2); // remove trailing ", "
  }

  private static void createCommentTable(Connection conn) throws Exception {
    String SQL = "CREATE TABLE JIRA_COMMENT (" +
          "comment_id bigint NOT NULL," +
          "issue_key varchar(11) NOT NULL, " +
          "author varchar NOT NULL, " +
          "body varchar NOT NULL, " +
          "creationDate timestamp NULL " +
          "CONSTRAINT pk primary key (comment_id))";
    PhoenixUtils.executeNoTableExistsThrow(conn, SQL);
  }
  
  private static void createAttachmentTable(Connection conn) throws Exception {
    String SQL = "CREATE TABLE JIRA_ATTACHMENT (" +
          "issue_key varchar(11) NOT NULL, " +
          "filename varchar NOT NULL, " +
          "contentUri varchar NOT NULL, " +
          "author varchar NULL, " +
          "mimeType varchar NULL, " +
          "size integer NOT NULL, " +
          "creationDate timestamp NULL " +
          "CONSTRAINT pk primary key (issue_key, filename))";
    PhoenixUtils.executeNoTableExistsThrow(conn, SQL);
  }

}
