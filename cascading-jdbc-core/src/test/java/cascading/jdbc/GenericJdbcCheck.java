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

import static org.junit.Assert.fail;

import org.junit.Before;

public class GenericJdbcCheck extends JDBCTestingBase
  {

  public final static String JDBC_URL_PROPERTY_NAME = "cascading.jdbcurl";

  public final static String JDBC_DRIVER_PROPERTY_NAME = "cascading.jdbcdriver";

  @Before
  public void setUp()
    {
    if ( System.getProperty( JDBC_DRIVER_PROPERTY_NAME ) == null || System.getProperty( JDBC_URL_PROPERTY_NAME ) == null )
      fail( String.format( "please set the '%s' and '%s' system properties", JDBC_DRIVER_PROPERTY_NAME, JDBC_URL_PROPERTY_NAME ) );

    setJdbcurl( System.getProperty( JDBC_URL_PROPERTY_NAME ) );
    setDriverName( System.getProperty( JDBC_DRIVER_PROPERTY_NAME ) );
    }
  }
