/*
 * Copyright, 1999-2012, SALESFORCE.com 
 * All Rights Reserved
 * Company Confidential
 */
package com.salesforce.jirachi.ingest;

import java.util.Properties;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Runs the jirachi import process
 */
public class CliMain {
    public static void main(String[] args) throws Exception {
      setProperties();
      JiraFetcher.fetchAndPersist();
      JiraTagger.addTags();
    }

    /*
    * Take the properties in the local file and push into system properties
    */
    public static void setProperties() {
	Properties p = new Properties();
	try {
		p.load(new FileInputStream("jirachi.properties"));
	} catch (IOException iox) {
		throw new RuntimeException(iox);
	}
	for (String name : p.stringPropertyNames()) {
	    String value = p.getProperty(name);
	    System.setProperty(name, value);
	}
    }
    
}
