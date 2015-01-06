/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.jdbc;

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
