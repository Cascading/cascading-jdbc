/*
 * Copyright (c) 2009 Concurrent, Inc.
 *
 * This work has been released into the public domain
 * by the copyright holder. This applies worldwide.
 *
 * In case this is not legally possible:
 * The copyright holder grants any entity the right
 * to use this work for any purpose, without any
 * conditions, unless such conditions are required by law.
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
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import cascading.CascadingException;
import cascading.jdbc.JDBCUtil;
import cascading.jdbc.TableDesc;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.util.Progressable;

/**
 * A OutputFormat that sends the reduce output to a SQL table.
 * <p/>
 * {@link DBOutputFormat} accepts &lt;key,value&gt; pairs, where key has a type
 * extending DBWritable. Returned {@link RecordWriter} writes <b>only the
 * key</b> to the database with a batch SQL query.
 */
public class DBOutputFormat<K extends DBWritable, V> implements OutputFormat<K, V>
  {
  private static final Log LOG = LogFactory.getLog( DBOutputFormat.class );

  /**
   * Initializes the reduce-part of the job with the appropriate output settings
   *
   * @param configuration       The Configuration object.
   * @param dbOutputFormatClass
   * @param tableDesc The TableDesc instance describing the table.
   */
  public static void setOutput( Configuration configuration, Class<? extends DBOutputFormat> dbOutputFormatClass, TableDesc tableDesc, String[] updateFields, int batchSize )
    {
    if( dbOutputFormatClass == null )
      configuration.set( "mapred.output.format.class", DBOutputFormat.class.getName() );
    else
      configuration.set( "mapred.output.format.class", dbOutputFormatClass.getName() );

    // writing doesn't always happen in reduce
    configuration.setBoolean( "mapred.map.tasks.speculative.execution", false );
    configuration.setBoolean( "mapred.reduce.tasks.speculative.execution", false );

    DBConfiguration dbConf = new DBConfiguration( configuration );

    dbConf.setOutputTableName( tableDesc.getTableName() );
    dbConf.setOutputFieldNames( tableDesc.getColumnNames() );
    dbConf.setOutputFieldTypes( tableDesc.getColumnDefs() );
    dbConf.setOutputPrimaryKeys( tableDesc.getPrimaryKeys() );

    if( updateFields != null )
      dbConf.setOutputUpdateFieldNames( updateFields );

    if( batchSize != -1 )
      dbConf.setBatchStatementsNum( batchSize );
    }

  /**
   * Constructs the query used as the prepared statement to insert data.
   *
   * @param table      the table to insert into
   * @param fieldNames the fields to insert into. If field names are unknown,
   *                   supply an array of nulls.
   */
  protected String constructInsertQuery( String table, String[] fieldNames )
    {
    if( fieldNames == null )
      throw new IllegalArgumentException( "Field names may not be null" );

    StringBuilder query = new StringBuilder();
    query.append( "INSERT INTO " ).append( table );
    if( fieldNames.length > 0 && fieldNames[ 0 ] != null )
      {
      query.append( " (" );
      for( int i = 0; i < fieldNames.length; i++ )
        {
        query.append( fieldNames[ i ] );
        if( i != fieldNames.length - 1 )
          query.append( "," );
        }
      query.append( ")" );
      }
    query.append( " VALUES (" );
    for( int i = 0; i < fieldNames.length; i++ )
      {
      query.append( "?" );
      if( i != fieldNames.length - 1 )
        query.append( "," );
      }
    query.append( ")" );

    return query.toString();
    }

  protected String constructUpdateQuery( String table, String[] fieldNames, String[] updateNames )
    {
    if( fieldNames == null )
      throw new IllegalArgumentException( "field names may not be null" );

    Set<String> updateNamesSet = new HashSet<String>();
    Collections.addAll( updateNamesSet, updateNames );

    StringBuilder query = new StringBuilder();

    query.append( "UPDATE " ).append( table );
    query.append( " SET " );

    if( fieldNames.length > 0 && fieldNames[ 0 ] != null )
      {
      int count = 0;
      for( int i = 0; i < fieldNames.length; i++ )
        {
        if( updateNamesSet.contains( fieldNames[ i ] ) )
          continue;

        if( count != 0 )
          query.append( "," );

        query.append( fieldNames[ i ] );
        query.append( " = ?" );
        count++;
        }
      }

    query.append( " WHERE " );

    if( updateNames.length > 0 && updateNames[ 0 ] != null )
      {
      for( int i = 0; i < updateNames.length; i++ )
        {
        query.append( updateNames[ i ] );
        query.append( " = ?" );

        if( i != updateNames.length - 1 )
          query.append( " and " );
        }
      }
    return query.toString();
    }

  /** {@inheritDoc} */
  public void checkOutputSpecs( FileSystem filesystem, JobConf job ) throws IOException
    {
    }

  /** {@inheritDoc} */
  public RecordWriter<K, V> getRecordWriter( FileSystem filesystem, JobConf job, String name, Progressable progress ) throws IOException
    {
    DBConfiguration dbConf = new DBConfiguration( job );

    String tableName = dbConf.getOutputTableName();
    String[] fieldNames = dbConf.getOutputFieldNames();
    String[] updateNames = dbConf.getOutputUpdateFieldNames();
    int batchStatements = dbConf.getBatchStatementsNum();

    Connection connection = dbConf.getConnection();

    TableDesc tableDesc = dbConf.toTableDesc();

    configureConnection( connection );
    JDBCUtil.createTableIfNotExists( connection, tableDesc );

    String sqlInsert = constructInsertQuery( tableName, fieldNames );
    PreparedStatement insertPreparedStatement;

    try
      {
      insertPreparedStatement = connection.prepareStatement( sqlInsert );
      insertPreparedStatement.setEscapeProcessing( true ); // should be on by default
      }
    catch( SQLException exception )
      {
      throw new IOException( "unable to create statement for: " + sqlInsert, exception );
      }

    String sqlUpdate = updateNames != null ? constructUpdateQuery( tableName, fieldNames, updateNames ) : null;
    PreparedStatement updatePreparedStatement;

    try
      {
      updatePreparedStatement = sqlUpdate != null ? connection.prepareStatement( sqlUpdate ) : null;
      }
    catch( SQLException exception )
      {
      throw new IOException( "unable to create statement for: " + sqlUpdate, exception );
      }

    return new DBRecordWriter( connection, insertPreparedStatement, updatePreparedStatement, batchStatements );
    }

  protected void configureConnection( Connection connection )
    {
    setAutoCommit( connection );
    }

  protected void setAutoCommit( Connection connection )
    {
    try
      {
      connection.setAutoCommit( false );
      }
    catch( SQLException exception )
      {
      throw new CascadingException( "unable to set auto commit", exception );
      }
    }

  /** A RecordWriter that writes the reduce output to a SQL table */
  protected class DBRecordWriter implements RecordWriter<K, V>
    {
    private final int statementsBeforeExecute;
    private Connection connection;
    private PreparedStatement insertStatement;
    private PreparedStatement updateStatement;
    private long statementsAdded = 0;
    private long insertStatementsCurrent = 0;
    private long updateStatementsCurrent = 0;

    protected DBRecordWriter( Connection connection, PreparedStatement insertStatement, PreparedStatement updateStatement, int statementsBeforeExecute )
      {
      this.connection = connection;
      this.insertStatement = insertStatement;
      this.updateStatement = updateStatement;
      this.statementsBeforeExecute = statementsBeforeExecute;
      }

    /** {@inheritDoc} */
    public void close( Reporter reporter ) throws IOException
      {
      try
        {
        if( insertStatement != null )
          executeBatch( insertStatement, insertStatementsCurrent );
        if( updateStatement != null )
          executeBatch( updateStatement, updateStatementsCurrent );
        }
      finally
        {
        JDBCUtil.closeConnection( connection );
        }
      }

    private void executeBatch( PreparedStatement preparedStatement, long currentCount ) throws IOException
      {
      try
        {
        if( currentCount != 0 )
          {
          LOG.info( "executing batch " + createBatchMessage( currentCount ) );
          int[] result = preparedStatement.executeBatch();
          int updatedRecords = 0;
          boolean hasUpdateCount = true;

          for( int value : result )
            {
            if( value == Statement.EXECUTE_FAILED )
              manageBatchProcessingError( "update failed", 0, new BatchProcessingException( "value=Statement.EXECUTE_FAILED" ) );
            else if( value == Statement.SUCCESS_NO_INFO )
              hasUpdateCount = false;

            updatedRecords = +value;
            }

          if( hasUpdateCount )
            LOG.info( "records:" + updatedRecords );

          // If no records matched the update query it's still a success. But if the number of updated statements
          // that ran isn't the expected count there's a problem.
          if( result.length != currentCount )
            manageBatchProcessingError( "update did not update same number of statements executed in batch, batch: " + currentCount
              + " updated: " + result.length, 0, new BatchProcessingException( "" ) );
          }

        connection.commit();
        }
      catch( SQLException exception )
        {
        manageBatchProcessingError( "unable to execute update batch", currentCount, exception );
        }

      }

    private String createBatchMessage( long currentStatements )
      {
      return String.format( "[totstmts: %d][crntstmts: %d][batch: %d]", statementsAdded, currentStatements, statementsBeforeExecute );
      }

    private void manageBatchProcessingError( String stateMessage, long currentStatements, SQLException exception ) throws IOException
      {
      String message = exception.getMessage();

      message = message.substring( 0, Math.min( 75, message.length() ) );

      int messageLength = exception.getMessage().length();
      String batchMessage = createBatchMessage( currentStatements );
      String template = "%s [msglength: %d]%s %s";
      String errorMessage = String.format( template, stateMessage, messageLength, batchMessage, message );

      LOG.error( errorMessage, exception.getNextException() );

      try
        {
        connection.rollback();
        connection.commit();
        }
      catch( SQLException sqlException )
        {
        LOG.error( "unable to rollback batch", sqlException );
        }

      throw new IOException( errorMessage, exception.getNextException() );
      }

    /** {@inheritDoc} */
    public synchronized void write( K key, V value ) throws IOException
      {
      try
        {
        if( value == null )
          {
          key.write( insertStatement );
          insertStatement.addBatch();
          insertStatementsCurrent++;
          }
        else
          {
          key.write( updateStatement );
          updateStatement.addBatch();
          updateStatementsCurrent++;
          }
        }
      catch( SQLException exception )
        {
        throw new IOException( "unable to add batch statement", exception );
        }

      statementsAdded++;

      if( statementsAdded % statementsBeforeExecute == 0 )
        {
        executeBatch( insertStatement, insertStatementsCurrent );
        executeBatch( updateStatement, updateStatementsCurrent );
        // reset counters after each batch
        insertStatementsCurrent = 0;
        updateStatementsCurrent = 0;
        }
      }
    }
  }
