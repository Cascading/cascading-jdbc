package cascading.provider.jdbc;

import org.junit.Before;

/**
 * This class runs the tests against an in network instance of apache derby:
 * http://db.apache.org/derby/
 * */
public class DerbyTest extends JDBCTestingBase
  {

  @Before
  public void setUp() throws Exception
    {
    System.setProperty( "derby.storage.rowLocking", "true" );
    System.setProperty( "derby.locks.monitor", "true" );
    System.setProperty( "derby.locks.deadlockTrace", "true" );
    System.setProperty( "derby.system.home", "build/derby" );

    setDriverName( "org.apache.derby.jdbc.EmbeddedDriver" );
    setJdbcurl( "jdbc:derby:testing;create=true" );

    }

  }
