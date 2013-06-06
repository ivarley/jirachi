package com.salesforce.jirachi.ingest;

import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Add search-term-based "tags" to the issue table (each as a column). Each search term is evaluated as
 * a LIKE clause against the summary field of the jira_issue table (case insensitive), and any records that 
 * match have 'is{tagName}' set to 'T'.
 * 
 * TODO: Why isn't it a BOOLEAN type? Because selecting that type in Squirrel client gives errors.  
 */
public class JiraTagger {

  /**
   * Strings to search on in summaries to apply tags. 
   * Limited wildcarding is allowed (% for any characters, . for one character), per SQL syntax.
   */
  static Map<String,List<String>> tags = new HashMap<String,List<String>>();
  /**
   * Strings to search on in summaries to remove tags.
   * Limited wildcarding is allowed (% for any characters, . for one character), per SQL syntax.
   */
  static Map<String,List<String>> tagExclusions = new HashMap<String,List<String>>();
  
  //TODO: move this to be pulled in from a file
  static {
    tags.put("isTest", Arrays.asList(
      "test",
      "junit",
      "hbase-it"));
    tags.put("isDoc", Arrays.asList(
      "ref manual", 
      " site",
      "site.xml",
      "website",
      "site target",
      "[site]",
      "[book]",
      "docbook",
      "[docs]",
      "doc improvement",
      "hbase-examples",
      "guide", 
      "book ",
      "hbase book",
      "book.xml",
      "the book",
      "of book",
      "to book",
      "javadoc",
      "document", 
      "copyright"));
    tags.put("isBuild", Arrays.asList(
      "pom",
      "maven",
      "mvn",
      "classpath",
      "compile",
      "build+error",
      "build+fix",
      "compilation",
      "assembly",
      "formatter",
      "hadoopqa",
      "findbug"));
    tags.put("isPort", Arrays.asList(
      "backport", 
      "back port", 
      "forward port", 
      "to 0.9", 
      "reapply",
      "port HBASE"));
    tags.put("isReplication", Arrays.asList(
      "replication"));
    tags.put("isSnapshot", Arrays.asList(
        "snapshot"));
    tagExclusions.put("isSnapshot", Arrays.asList(
        "snapshotatcreation",
        "netty",
        "pom.xml",
        "mvn"));
    tags.put("isProto", Arrays.asList(
        "protos",
        "protocol buffer",
        "protobuf", 
        "proto ", 
        "hbase-protocol",
        "pb-",
        "to pb",
        "pb objects",
        "pb definitions",
        "pb conversion",
        "enforce pb"));
    tags.put("isModule", Arrays.asList(
      "hbase-common",
      "hbase-client",
      "module naming",
      "multi-module",
      "multimodule",
      "module split",
      "modularization",
      "consistent package"));
    tags.put("isCompaction", Arrays.asList(
      "compact"));
    tags.put("isBulk", Arrays.asList(
        "bulkload",
        "bulk load",
        "bulk delet"));
    tags.put("isSecurity", Arrays.asList(
      "secur"));
    tags.put("isMetrics", Arrays.asList(
      "metric"));
    tags.put("isAssignment", Arrays.asList(
      "assignment",
      "assign%region",
      "region%assign"));
    tags.put("isHadoop2", Arrays.asList(
      "hadoop 2",
      "hadoop2",
      "hadoop-2",
      "hadoop 1 and 2",
      "hadoop-0.20.205 and 0.22.0",
      "hadoop-profile=2.0",
      "hadoop 0.22",
      "hadoop 0.23"));
  }

  /**
   * Get a list of all the tag names, to be used in the create table statement
   * @return
   */
  public static Collection<String> getTagNames() {
    return tags.keySet();
  }

  /**
   * Update the database to set each tag, based on the search terms.
   */
  public static void addTags() throws Exception {
    Connection conn = PhoenixUtils.getPhoenixConnection();

    try {
      for (String tagName : tags.keySet()){
        List<String> searchTerms = tags.get(tagName);
        StringBuilder b = new StringBuilder("UPSERT INTO Jira_Issue (id, ").append(tagName);
        b.append(") SELECT id, 'T' FROM Jira_Issue WHERE (");
        // apply tags
        for (String term : searchTerms) {
          b.append("lower(summary) like '%").append(term.toLowerCase()).append("%' OR ");
        }
        b.append(" 1 = 0)"); // cap it off series of ORs with expression that always evaluates to false 
        // apply tag exclusions
        List<String> exclusionTerms = tagExclusions.get(tagName);
        if (exclusionTerms != null){
          b.append(" AND (");
          for (String exclusionTerm : exclusionTerms){
            b.append("lower(summary) not like '%").append(exclusionTerm.toLowerCase()).append("%' AND ");
          }
          b.append(" 1 = 1)"); // cap off series of ANDs with expression that always evaluates to true
        }
        PhoenixUtils.execute(conn, b.toString());
      }
    } finally {
      conn.close();
    }
  }

}
