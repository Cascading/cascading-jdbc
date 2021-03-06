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

import java.io.IOException;
import java.util.Map;
import java.util.UUID;

import cascading.flow.FlowProcess;
import cascading.jdbc.db.DBConfiguration;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.hadoop.Hfs;
import cascading.tuple.TupleEntryCollector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class and {@link RedshiftScheme} manage the ability to read and write data to Amazon's Redshift via EMR.
 * Because Redshift data is loaded into Redshift via S3 but read from it via JDBC both these classes wrap the
 * pairing of an HFS {@link Tap} (for writing) and a JDBC {@link Tap} for reading behind one {@link cascading.scheme.Scheme}
 * object.
 */
public class RedshiftTap extends JDBCTap
  {

  private static final Logger LOG = LoggerFactory.getLogger( RedshiftTap.class );

  public static final String DB_DRIVER = "org.postgresql.Driver";

  private AWSCredentials awsCredentials;
  private RedshiftScheme redshiftScheme;
  private Hfs hfsStagingDir;
  private String s3WorkingDir;
  private boolean keepDebugHfsData;
  private boolean useDirectInsert;


  /**
   * Redshift tap to stage data to S3 and then issue a JDBC COPY command to specified Redshift table
   *
   * @param sinkMode use {@link SinkMode#REPLACE} to drop Redshift table before loading;
   *                 {@link SinkMode#UPDATE} to not drop table for incremental loading
   */
  public RedshiftTap( String connectionUrl, String username, String password, String hfsStagingDir, AWSCredentials awsCredentials, RedshiftTableDesc redshiftTableDesc, RedshiftScheme redshiftScheme, SinkMode sinkMode, boolean keepDebugHfsData, boolean useDirectInsert )
    {
    super( connectionUrl, username, password, DB_DRIVER, redshiftTableDesc, redshiftScheme, sinkMode );
    this.redshiftScheme = redshiftScheme;
    String workingDirPath = hfsStagingDir + "/" + UUID.randomUUID();
    this.s3WorkingDir = workingDirPath.replaceAll( "s3n://", "s3://" );
    this.hfsStagingDir = new Hfs( redshiftScheme.getTextDelimited(), workingDirPath );
    this.awsCredentials = awsCredentials;
    this.keepDebugHfsData = keepDebugHfsData;
    this.useDirectInsert = useDirectInsert;
    LOG.info( "created {} ", toString() );
    }

  /**
   * Redshift tap to stage data to S3 and then issue a JDBC COPY command to specified Redshift table
   *
   * @param sinkMode use {@link SinkMode#REPLACE} to drop Redshift table before loading;
   *                 {@link SinkMode#UPDATE} to not drop table for incremental loading
   */
  public RedshiftTap( String connectionUrl, String username, String password, String hfsStagingDir, AWSCredentials awsCredentials, RedshiftTableDesc redshiftTableDesc, RedshiftScheme redshiftScheme, SinkMode sinkMode )
    {
    this( connectionUrl, username, password, hfsStagingDir, awsCredentials, redshiftTableDesc, redshiftScheme, sinkMode, false, true );
    }

  /**
   * Simplified constructor for testing
   */
  protected RedshiftTap( String connectionUrl, RedshiftTableDesc redshiftTableDesc, RedshiftScheme redshiftScheme, SinkMode sinkMode )
    {
    this( connectionUrl, null, null, null, null, redshiftTableDesc, redshiftScheme, sinkMode, false, true );
    }

  /**
   * Simplified constructor for testing
   */
  protected RedshiftTap( String connectionUrl, RedshiftScheme redshiftScheme )
    {
    this( connectionUrl, null, null, null, null, null, redshiftScheme, null, false, true );
    }

  @Override
  public void sourceConfInit( FlowProcess<? extends Configuration> process, Configuration configuration )
    {
    if( username == null )
      DBConfiguration.configureDB( configuration, driverClassName, connectionUrl );
    else
      DBConfiguration.configureDB( configuration, driverClassName, connectionUrl, username, password );

    super.sourceConfInit( process, configuration );
    }

  @Override
  public void sinkConfInit( FlowProcess<? extends Configuration> process, Configuration conf )
    {
    if (!useDirectInsert) {
      // if we haven't set the credentials beforehand try to set them from the job conf
      if( awsCredentials.equals( AWSCredentials.RUNTIME_DETERMINED ) )
        {
        String accessKey = conf.get( "fs.s3n.awsAccessKeyId", null );
        String secretKey = conf.get( "fs.s3n.awsSecretAccessKey", null );
        awsCredentials = new AWSCredentials( accessKey, secretKey );
        }
      // make the credentials to be used available to the JobConf if they were set differently
      conf.set( "fs.s3n.awsAccessKeyId", awsCredentials.getAwsAccessKey() );
      conf.set( "fs.s3n.awsSecretAccessKey", awsCredentials.getAwsSecretKey() );
    }
    super.sinkConfInit( process, conf );
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<? extends Configuration> flowProcess, OutputCollector outputCollector ) throws IOException
    {
    // force a table creation if one does not exist
    LOG.info( "creating db table: " + getTableName() );
    super.createResource( flowProcess );
    if( useDirectInsert )
      {
      return super.openForWrite( flowProcess, outputCollector );
      }
    else
      {
      LOG.info( "Creating scratch dir: " + hfsStagingDir.getIdentifier() );
      hfsStagingDir.createResource( flowProcess );
      return hfsStagingDir.openForWrite( flowProcess );
      }
    }

  @Override
  public boolean createResource( Configuration configuration ) throws IOException
    {
    LOG.info( "creating resources" );
    boolean createSuccess = true;
    if( !useDirectInsert )
      {
      LOG.info( "creating hfs scratch space: {}", hfsStagingDir.getIdentifier() );
      createSuccess = hfsStagingDir.createResource( configuration );
      }
    if( createSuccess )
      {
      LOG.info( "creating DB table: {}", super.getIdentifier() );
      createSuccess = super.createResource( configuration );
      }
    return createSuccess;
    }

  @Override
  public boolean deleteResource( Configuration configuration ) throws IOException
    {
    LOG.info( "deleting resources" );
    boolean deleteSuccsess;
    LOG.info( "deleting DB table: {}", super.getIdentifier() );
    deleteSuccsess = super.deleteResource( configuration );
    if( deleteSuccsess && hfsStagingDir.resourceExists( configuration ) )
      {
      LOG.info( "deleting hfs scratch space: {}", hfsStagingDir.getIdentifier() );
      deleteSuccsess = hfsStagingDir.deleteResource( configuration );
      }
    return deleteSuccsess;
    }

  @Override
  public boolean commitResource( Configuration configuration ) throws IOException
    {
    if( !useDirectInsert )
      {
      String copyCommand = buildCopyFromS3Command();
      try
        {
        int results = super.executeUpdate( copyCommand );
        if( results != 0 )
          LOG.info( "Copy return code: {} ( expected: 0 )", results );
        }
      finally
        {
        // clean scratch resources even if load failed.
        if( !keepDebugHfsData && hfsStagingDir.resourceExists( configuration ) )
          hfsStagingDir.deleteResource( configuration );
        }
      }
    return true;
    }

  @Override
  public long getModifiedTime( Configuration configuration ) throws IOException
    {
    if( hfsStagingDir.resourceExists( configuration ) )
      return hfsStagingDir.getModifiedTime( configuration );
    return super.getModifiedTime( configuration );
    }

  public boolean isUseDirectInsert()
    {
    return useDirectInsert;
    }

  public String buildCopyFromS3Command()
    {
    return String.format( "COPY %s from '%s' %s %s ;",
      redshiftScheme.getRedshiftTableDesc().getTableName(),
      s3WorkingDir,
      buildAuthenticationOptions(),
      buildCopyOptions() );
    }

  protected String buildAuthenticationOptions()
    {
    return String.format( " CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' ",
      awsCredentials.getAwsAccessKey(),
      awsCredentials.getAwsSecretKey() );
    }

  private String buildCopyOptions()
    {
    StringBuilder builder = new StringBuilder();
    for( Map.Entry<RedshiftFactory.CopyOption, String> copyOption : redshiftScheme.getCopyOptions().entrySet() )
      {
      builder.append( " " );
      if( copyOption.getValue() == null )
        builder.append( copyOption.getKey().toString() );
      else
        builder.append( copyOption.getKey().toString() ).append( " " ).append( copyOption.getKey().getArguments( copyOption.getValue() ) );
      }
    return builder.toString();
    }

  @Override
  public String toString()
    {
    if( getIdentifier() != null )
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[->\"" + hfsStagingDir.getIdentifier() + "\"->\"" + super.getIdentifier() + "\"]"; // sanitize
    else
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[no more info]";
    }

  }
