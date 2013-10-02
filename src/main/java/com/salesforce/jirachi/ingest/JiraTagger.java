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
        b.append(") SELECT id, true FROM Jira_Issue WHERE (");
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
