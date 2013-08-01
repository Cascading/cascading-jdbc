package com.twitter.maple.jdbc;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;


/**
 * This class runs the tests against an instance of hsql.
 * */
public class HSQLTest extends JDBCTestingBase
  {


	@Before
  public void setUp() throws IOException
    {

    	
    	this.jdbcurl =  "jdbc:hsqldb:mem://testing_" + name.getMethodName() + ";hsqldb.tx=mvcc";
    	this.driverName = "org.hsqldb.jdbcDriver";
	    
    }
	
  }
