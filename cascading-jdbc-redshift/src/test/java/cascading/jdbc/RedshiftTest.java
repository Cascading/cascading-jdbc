/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
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

/**
 * Tests against Postgres database since that's the Redshift API .
 **/

import java.util.Properties;

import cascading.tap.SinkMode;
import cascading.tuple.Fields;
import org.junit.Before;

public class RedshiftTest extends JDBCTestingBase
  {

  @Before
  public void setUp()
    {
    setDriverName( RedshiftTap.DB_DRIVER );
    setJdbcurl( System.getProperty( "cascading.jdbcurl" ) );
    setJDBCFactory( new RedshiftFactory() );
    }

  @Override
  protected RedshiftScheme getNewJDBCScheme( Fields fields, String[] columnNames )
    {
    return new RedshiftScheme( inputFormatClass, fields, columnNames );
    }

  @Override
  protected RedshiftScheme getNewJDBCScheme( String[] columns, String[] orderBy, String[] updateBy )
    {
    return new RedshiftScheme( columns, orderBy, updateBy );
    }

  @Override
  protected RedshiftScheme getNewJDBCScheme( String[] columnsNames, String contentsQuery, String countStarQuery )
    {
    return new RedshiftScheme( columnsNames, contentsQuery, countStarQuery );
    }

  @Override
  protected RedshiftTableDesc getNewTableDesc( String tableName, String[] columnNames, String[] columnDefs, String[] primaryKeys )
    {
    return new RedshiftTableDesc( tableName, columnNames, columnDefs, null, null );
    }

  @Override
  protected RedshiftTap getNewJDBCTap( TableDesc tableDesc, JDBCScheme jdbcScheme, SinkMode sinkMode )
    {
    return new RedshiftTap( jdbcurl, (RedshiftTableDesc) tableDesc, (RedshiftScheme) jdbcScheme, sinkMode );
    }

  @Override
  protected RedshiftTap getNewJDBCTap( JDBCScheme jdbcScheme )
    {
    return new RedshiftTap( jdbcurl, (RedshiftScheme) jdbcScheme );
    }

  @Override
  protected SinkMode getSinkModeForReset()
    {
    return SinkMode.REPLACE;
    }

  @Override
  protected Properties createProperties()
    {
    Properties properties = super.createProperties();
    properties.put( RedshiftFactory.PROTOCOL_USE_DIRECT_INSERT, "true" );
    return properties;
    }
  }


