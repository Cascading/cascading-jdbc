package com.twitter.maple.jdbc;

import java.io.PrintWriter;
import java.net.InetAddress;

import org.apache.derby.drda.NetworkServerControl;
import org.junit.After;
import org.junit.Before;


/**
 * This class runs the tests against an in network instance of apache derby: http://db.apache.org/derby/
 * */
public class DerbyTest extends JDBCTestingBase {
	
	private NetworkServerControl server;
	
	@Before
  public void setUp() throws Exception
    {
		System.setProperty("derby.storage.rowLocking", "true");
		System.setProperty("derby.stream.error.file", "build/derby.log");
		System.setProperty("derby.locks.monitor", "true");
		System.setProperty("derby.locks.deadlockTrace", "true");
		System.setProperty("derby.system.home", "build/derby");
		
		System.setProperty("derby.drda.startNetworkServer", "true");
		
		server = new NetworkServerControl(InetAddress.getByName("localhost"), 9002);
		PrintWriter consoleWriter = new java.io.PrintWriter(System.out, true);
		server.start(consoleWriter);
		
		
		setDriverName("org.apache.derby.jdbc.ClientDriver");
		setJdbcurl("jdbc:derby://localhost:9002/testing;create=true");		    
 
    }
	
	@After
	public void tearDown() throws Exception
	  {
		server.shutdown();
	  }

	
}
