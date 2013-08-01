package com.twitter.maple.jdbc;

import org.h2.tools.Server;
import org.junit.Before;

public class H2Test extends JDBCTestingBase {
	
	Server server;
	
	@Before
	public void setUp()
  	{
		  this.driverName = "org.h2.Driver";
			this.jdbcurl = "jdbc:h2:mem:testing_" + name.getMethodName()+ ";DB_CLOSE_DELAY=-1;MVCC=true";
 	  }

}
