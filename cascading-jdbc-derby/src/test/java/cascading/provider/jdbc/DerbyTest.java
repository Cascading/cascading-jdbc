package cascading.provider.jdbc;

import java.io.PrintWriter;
import java.net.InetAddress;

import org.apache.derby.drda.NetworkServerControl;
import org.junit.After;
import org.junit.Before;

/**
 * This class runs the tests against an in network instance of apache derby:
 * http://db.apache.org/derby/
 * */
public class DerbyTest extends JDBCTestingBase
  {
  
  private final int PORT = 9006;
  private NetworkServerControl serverControl;
  

  
  @Before
  public void setUp() throws Exception
    {
    System.setProperty( "derby.storage.rowLocking", "true" );
    System.setProperty( "derby.locks.monitor", "true" );
    System.setProperty( "derby.locks.deadlockTrace", "true" );
    System.setProperty( "derby.system.home", "build/derby" );

    serverControl = new NetworkServerControl(InetAddress.getByName("localhost"), PORT);
    serverControl.start(new PrintWriter(System.out,true));
    
    setDriverName( "org.apache.derby.jdbc.ClientDriver" );
    setJdbcurl( String.format("jdbc:derby://localhost:%s/testing;create=true", PORT) );

    }

  
  @After
  public void tearDown() throws Exception
    {
    serverControl.shutdown();
    }
  }
