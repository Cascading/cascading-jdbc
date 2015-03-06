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

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.jdbc.db;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import cascading.jdbc.TableDesc;
import cascading.util.Util;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * A container for configuration property names for jobs with DB input/output. <br> The configuration can be
 * configured using the static methods in this class, {@link DBInputFormat}, and {@link
 * DBOutputFormat}. <p/> Alternatively, the properties can be set in the configuration with proper
 * values.
 */
public class DBConfiguration
  {

  private static final Log LOG = LogFactory.getLog( DBConfiguration.class );

  private static final String SEPARATOR = ":";

  /** The JDBC Driver class name */
  public static final String DRIVER_CLASS_PROPERTY = "mapred.jdbc.driver.class";

  /** JDBC Database access URL */
  public static final String URL_PROPERTY = "mapred.jdbc.url";

  /** User name to access the database */
  public static final String USERNAME_PROPERTY = "mapred.jdbc.username";

  /** Password to access the database */
  public static final String PASSWORD_PROPERTY = "mapred.jdbc.password";

  /** Input table name */
  public static final String INPUT_TABLE_NAME_PROPERTY = "mapred.jdbc.input.table.name";

  /** Field names in the Input table */
  public static final String INPUT_FIELD_NAMES_PROPERTY = "mapred.jdbc.input.field.names";

  /** WHERE clause in the input SELECT statement */
  public static final String INPUT_CONDITIONS_PROPERTY = "mapred.jdbc.input.conditions";

  /** ORDER BY clause in the input SELECT statement */
  public static final String INPUT_ORDER_BY_PROPERTY = "mapred.jdbc.input.orderby";

  /** Whole input query, exluding LIMIT...OFFSET */
  public static final String INPUT_QUERY = "mapred.jdbc.input.query";

  /** The number of records to LIMIT, useful for testing */
  public static final String INPUT_LIMIT = "mapred.jdbc.input.limit";

  /** Input query to get the count of records */
  public static final String INPUT_COUNT_QUERY = "mapred.jdbc.input.count.query";

  /** Class name implementing DBWritable which will hold input tuples */
  public static final String INPUT_CLASS_PROPERTY = "mapred.jdbc.input.class";

  /** Boolean to include table name alias in the input SELECT statement */
  public static final String INPUT_TABLE_ALIAS = "mapred.jdbc.input.table.alias";

  /** Output table name */
  public static final String OUTPUT_TABLE_NAME_PROPERTY = "mapred.jdbc.output.table.name";

  /** Field names in the Output table */
  public static final String OUTPUT_FIELD_NAMES_PROPERTY = "mapred.jdbc.output.field.names";

  /** Field types in the Output table */
  public static final String OUTPUT_FIELD_TYPES_PROPERTY = "mapred.jdbc.output.field.types";

  /** Field types in the Output table */
  public static final String OUTPUT_PRIMARY_KEYS_PROPERTY = "mapred.jdbc.output.tableprimarykeys";

  /** Field names in the Output table */
  public static final String OUTPUT_UPDATE_FIELD_NAMES_PROPERTY = "mapred.jdbc.output.update.field.names";

  /** The number of statements to batch before executing */
  public static final String BATCH_STATEMENTS_PROPERTY = "mapred.jdbc.batch.statements.num";

  /** The number of splits allowed, becomes max concurrent reads. */
  public static final String CONCURRENT_READS_PROPERTY = "mapred.jdbc.concurrent.reads.num";

  private Configuration configuration;

  DBConfiguration( Configuration job )
    {
    this.configuration = job;
    }

  /**
   * Sets the DB access related fields in the Configuration.
   *
   * @param job         the configuration
   * @param driverClass JDBC Driver class name
   * @param dbUrl       JDBC DB access URL.
   * @param userName    DB access username
   * @param passwd      DB access passwd
   */
  public static void configureDB( Configuration job, String driverClass, String dbUrl, String userName, String passwd )
    {
    job.set( DRIVER_CLASS_PROPERTY, driverClass );
    job.set( URL_PROPERTY, dbUrl );

    if( userName != null )
      job.set( USERNAME_PROPERTY, userName );

    if( passwd != null )
      job.set( PASSWORD_PROPERTY, passwd );
    }

  /**
   * Sets the DB access related fields in the Configuration.
   *
   * @param job         the configuration
   * @param driverClass JDBC Driver class name
   * @param dbUrl       JDBC DB access URL.
   */
  public static void configureDB( Configuration job, String driverClass, String dbUrl )
    {
    configureDB( job, driverClass, dbUrl, null, null );
    }

  /**
   * Returns a connection object to the DB
   *
   * @throws ClassNotFoundException
   * @throws SQLException
   */
  Connection getConnection() throws IOException
    {
    try
      {
      Class.forName( configuration.get( DBConfiguration.DRIVER_CLASS_PROPERTY ) );
      }
    catch( ClassNotFoundException exception )
      {
      throw new IOException( "unable to load database driver", exception );
      }
    LOG.info( "opening db connection: " + configuration.get( DBConfiguration.URL_PROPERTY ) );
    try
      {
      if( configuration.get( DBConfiguration.USERNAME_PROPERTY ) == null )
        return DriverManager.getConnection( configuration.get( DBConfiguration.URL_PROPERTY ) );

      else
        {
        return DriverManager.getConnection( configuration.get( DBConfiguration.URL_PROPERTY ),
          configuration.get( DBConfiguration.USERNAME_PROPERTY ),
          configuration.get( DBConfiguration.PASSWORD_PROPERTY ) );
        }
      }
    catch( SQLException exception )
      {
      throw new IOException( "unable to create connection", exception );
      }
    }

  String getInputTableName()
    {
    return configuration.get( DBConfiguration.INPUT_TABLE_NAME_PROPERTY );
    }

  void setInputTableName( String tableName )
    {
    configuration.set( DBConfiguration.INPUT_TABLE_NAME_PROPERTY, tableName );
    }

  String[] getInputFieldNames()
    {
    return configuration.getStrings( DBConfiguration.INPUT_FIELD_NAMES_PROPERTY );
    }

  void setInputFieldNames( String... fieldNames )
    {
    configuration.setStrings( DBConfiguration.INPUT_FIELD_NAMES_PROPERTY, fieldNames );
    }

  String getInputConditions()
    {
    return configuration.get( DBConfiguration.INPUT_CONDITIONS_PROPERTY );
    }

  void setInputConditions( String conditions )
    {
    if( conditions != null && conditions.length() > 0 )
      {
      configuration.set( DBConfiguration.INPUT_CONDITIONS_PROPERTY, conditions );
      }
    }

  String getInputOrderBy()
    {
    return configuration.get( DBConfiguration.INPUT_ORDER_BY_PROPERTY );
    }

  void setInputOrderBy( String orderby )
    {
    if( orderby != null && orderby.length() > 0 )
      {
      configuration.set( DBConfiguration.INPUT_ORDER_BY_PROPERTY, orderby );
      }
    }

  Boolean getTableAlias()
    {
    return configuration.getBoolean( DBConfiguration.INPUT_TABLE_ALIAS, true );
    }

  void setTableAlias( Boolean alias )
    {
    configuration.setBoolean( DBConfiguration.INPUT_TABLE_ALIAS, alias );
    }

  String getInputQuery()
    {
    return configuration.get( DBConfiguration.INPUT_QUERY );
    }

  void setInputQuery( String query )
    {
    if( query != null && query.length() > 0 ){ configuration.set( DBConfiguration.INPUT_QUERY, query ); }
    }

  long getInputLimit()
    {
    return configuration.getLong( DBConfiguration.INPUT_LIMIT, -1 );
    }

  void setInputLimit( long limit )
    {
    configuration.setLong( DBConfiguration.INPUT_LIMIT, limit );
    }

  String getInputCountQuery()
    {
    return configuration.get( DBConfiguration.INPUT_COUNT_QUERY );
    }

  void setInputCountQuery( String query )
    {
    if( query != null && query.length() > 0 )
      {
      configuration.set( DBConfiguration.INPUT_COUNT_QUERY, query );
      }
    }

  Class<?> getInputClass()
    {
    return configuration
      .getClass( DBConfiguration.INPUT_CLASS_PROPERTY, DBInputFormat.NullDBWritable.class );
    }

  void setInputClass( Class<? extends DBWritable> inputClass )
    {
    configuration.setClass( DBConfiguration.INPUT_CLASS_PROPERTY, inputClass, DBWritable.class );
    }

  String getOutputTableName()
    {
    return configuration.get( DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY );
    }

  void setOutputTableName( String tableName )
    {
    configuration.set( DBConfiguration.OUTPUT_TABLE_NAME_PROPERTY, tableName );
    }

  String[] getOutputFieldNames()
    {
    return configuration.getStrings( DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY );
    }

  void setOutputFieldNames( String... fieldNames )
    {
    configuration.setStrings( DBConfiguration.OUTPUT_FIELD_NAMES_PROPERTY, fieldNames );
    }

  String[] getOutputFieldTypes()
    {
    return configuration.get( DBConfiguration.OUTPUT_FIELD_TYPES_PROPERTY ).split( SEPARATOR );
    }

  void setOutputFieldTypes( String... fieldTypes )
    {
    configuration.set( DBConfiguration.OUTPUT_FIELD_TYPES_PROPERTY, Util.join( fieldTypes, SEPARATOR ) );
    }

  String[] getOutputUpdateFieldNames()
    {
    return configuration.getStrings( DBConfiguration.OUTPUT_UPDATE_FIELD_NAMES_PROPERTY );
    }

  void setOutputUpdateFieldNames( String... fieldNames )
    {
    configuration.setStrings( DBConfiguration.OUTPUT_UPDATE_FIELD_NAMES_PROPERTY, fieldNames );
    }

  int getBatchStatementsNum()
    {
    return configuration.getInt( DBConfiguration.BATCH_STATEMENTS_PROPERTY, 1000 );
    }

  void setBatchStatementsNum( int batchStatementsNum )
    {
    configuration.setInt( DBConfiguration.BATCH_STATEMENTS_PROPERTY, batchStatementsNum );
    }

  String [] getOutputPrimaryKeys()
    {
    String primaryKeys = configuration.get( OUTPUT_PRIMARY_KEYS_PROPERTY );
    if( primaryKeys == null )
      return null;
    return primaryKeys.split( SEPARATOR );
    }

  void setOutputPrimaryKeys( String [] primaryKeys )
    {
    if( primaryKeys != null )
      configuration.set( OUTPUT_PRIMARY_KEYS_PROPERTY, Util.join( primaryKeys, SEPARATOR ) );
    }

  int getMaxConcurrentReadsNum()
    {
    return configuration.getInt( DBConfiguration.CONCURRENT_READS_PROPERTY, 0 );
    }

  void setMaxConcurrentReadsNum( int maxConcurrentReads )
    {
    if( maxConcurrentReads < 0 )
      throw new IllegalArgumentException( "maxConcurrentReads must be a positive value" );

    configuration.setInt( DBConfiguration.CONCURRENT_READS_PROPERTY, maxConcurrentReads );
    }

  TableDesc toTableDesc()
    {
    return new TableDesc( getOutputTableName(), getOutputFieldNames(), getOutputFieldTypes(), getOutputPrimaryKeys() );
    }

  }
