package com.salesforce.jirachi.ingest;

import java.io.File;
import java.sql.Connection;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

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
  static Map<String,List<String>> tags = loadTags("include.json");
  /**
   * Strings to search on in summaries to remove tags.
   * Limited wildcarding is allowed (% for any characters, . for one character), per SQL syntax.
   */
  static Map<String,List<String>> tagExclusions = loadTags("exclude.json");
  
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

  /**
   * Load the tags and search expressions from json, return them as a map of lists
   */
  public static Map<String, List<String>> loadTags(String fileName)  {
    Map<String, List<String>> result = Maps.newLinkedHashMap(); // keep insertion order
    try {
      ObjectMapper m = new ObjectMapper();
      JsonNode root = m.readValue(new File(fileName), JsonNode.class);
      Iterator<Entry<String,JsonNode>> tags = root.getFields();
      while (tags.hasNext()){
        Entry<String,JsonNode> tag = tags.next();
        String tagName = tag.getKey();
        Iterator<JsonNode> matchers = tag.getValue().getElements();
        List<String> matcherValues = Lists.newArrayList();
        while (matchers.hasNext()){
          JsonNode matcher = matchers.next();
          matcherValues.add(matcher.getTextValue());
        }
        result.put(tagName, matcherValues);
      }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
    return result;
  }

}
