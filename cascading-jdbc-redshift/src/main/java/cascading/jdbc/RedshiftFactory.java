/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link RedshiftFactory} is a factory class to create {@link RedshiftTap}s
 * and {@link RedshiftScheme}s. The class is meant to be used by <a
 * href="http://www.cascading.org/lingual/">lingual</a> for dynamically creating
 * Taps and Schemes, so that redshift can be used as a <a
 * href="http://docs.cascading.org/lingual/1.0/#_creating_a_data_provider"
 * >provider</a> within lingual.
 */
public class RedshiftFactory extends JDBCFactory
  {
  /** LOGGER */
  private static final Logger LOG = LoggerFactory.getLogger( RedshiftFactory.class );

  /** environment variable for the aws access key */
  private static final String SYSTEM_AWS_ACCESS_KEY = "AWS_ACCESS_KEY";

  /** environment variable for the aws secret key */
  private static final String SYSTEM_AWS_SECRET_KEY = "AWS_SECRET_KEY";

  public static final String PROTOCOL_S3_OUTPUT_PATH = "s3outputpath";
  public static final String PROTOCOL_AWS_ACCESS_KEY = "awsacceskey";
  public static final String PROTOCOL_AWS_SECRET_KEY = "awssecretkey";
  ;
  public static final String PROTOCOL_KEEP_DEBUG_HFS_DATA = "keepdebughfsdata";
  public static final String PROTOCOL_USE_DIRECT_INSERT = "usedirectinsert";

  public static final String FORMAT_DISTRIBUTION_KEY = "distributionkey";
  public static final String FORMAT_SORT_KEYS = "sortkeys";
  public static final String FORMAT_COPY_OPTIONS_PREFIX = "copyoptions.";
  public static final String FORMAT_FIELD_DELIMITER = "fielddelimiter";
  public static final String FORMAT_QUOTE_CHARACTER = "quotecharacter";

  @SuppressWarnings("unused")
  public String getDescription()
    {
    return getClass().getSimpleName();
    }

  @SuppressWarnings("rawtypes")
  public Scheme createScheme( String format, Fields fields, Properties formatProperties )
    {
    LOG.info( "creating RedshiftScheme for format {} with fields {} and properties {}", format, fields, formatProperties );

    String delimiter = formatProperties.getProperty( FORMAT_FIELD_DELIMITER, RedshiftScheme.DEFAULT_DELIMITER );
    String quoteCharacter = formatProperties.getProperty( FORMAT_QUOTE_CHARACTER, RedshiftScheme.DEFAULT_QUOTE );

    RedshiftTableDesc redshiftTableDesc = createTableDescFromProperties( formatProperties );

    Map<CopyOption, String> copyOptions = extractCopyOptions( formatProperties, FORMAT_COPY_OPTIONS_PREFIX );

    return new RedshiftScheme( fields, redshiftTableDesc, delimiter, quoteCharacter, copyOptions );
    }

  @SuppressWarnings("rawtypes")
  public Tap createTap( String protocol, Scheme scheme, String identifier, SinkMode sinkMode, Properties protocolProperties )
    {
    LOG.info( "creating RedshiftTap with properties {} in mode {}", protocolProperties, sinkMode );

    String jdbcUserProperty = protocolProperties.getProperty( PROTOCOL_JDBC_USER );
    String jdbcPasswordProperty = protocolProperties.getProperty( PROTOCOL_JDBC_PASSWORD );

    String jdbcUser = null;
    if( jdbcUserProperty != null && !jdbcUserProperty.isEmpty() )
      jdbcUser = jdbcUserProperty;

    String jdbcPassword = null;
    if( jdbcPasswordProperty != null && !jdbcPasswordProperty.isEmpty() )
      jdbcPassword = jdbcPasswordProperty;

    String hfsStagingDir = "/tmp";

    AWSCredentials credentials = determineAwsCredentials( protocolProperties );

    boolean keepDebugHdfsData = Boolean.parseBoolean( protocolProperties.getProperty( PROTOCOL_KEEP_DEBUG_HFS_DATA ) );
    boolean useDirectInsert = Boolean.parseBoolean( protocolProperties.getProperty( PROTOCOL_USE_DIRECT_INSERT, "true" ) );

    //return new RedshiftTap( identifier, jdbcUser, jdbcPassword, hfsStagingDir, credentials, (RedshiftScheme)scheme, mode, keepDebugHdfsData, useDirectInsert );
    //String connectionUrl, String username, String password, String driverClassName, TableDesc tableDesc, JDBCScheme
   // scheme,
   //   SinkMode sinkMode

    RedshiftTableDesc redshiftTableDesc = createTableDescFromProperties( protocolProperties ) ;
    JDBCScheme jdbcScheme = (JDBCScheme) scheme;

    Fields sinkFields = jdbcScheme.getSinkFields();
    if( !redshiftTableDesc.hasRequiredTableInformation() && sinkFields != Fields.UNKNOWN && sinkFields != Fields.ALL && sinkFields != null
      && sinkFields.getTypes() != null )
      {
      LOG.debug( "tabledesc information incomplete, falling back to sink-fields {}", jdbcScheme.getSinkFields() );
      redshiftTableDesc.completeFromFields( jdbcScheme.getSinkFields() );
      ( (JDBCScheme) scheme ).setColumns( redshiftTableDesc.getColumnNames() );
      }

    // users can overwrite the sink mode.
    String sinkModeProperty = protocolProperties.getProperty( PROTOCOL_SINK_MODE );
    if( sinkModeProperty != null && !sinkModeProperty.isEmpty() )
      sinkMode = SinkMode.valueOf( sinkModeProperty );

    //return new JDBCTap( identifier, jdbcUser, jdbcPassword, RedshiftTap.DB_DRIVER, redshiftTableDesc, jdbcScheme, sinkMode );
    //  RedshiftTableDesc redshiftTableDesc, RedshiftScheme redshiftScheme, SinkMode sinkMode, boolean keepDebugHfsData, boolean useDirectInsert, JDBCScheme jdbcScheme )=

    //RedshiftScheme redshiftScheme = new RedshiftScheme( redshiftTableDesc );

    return new RedshiftTap( identifier, jdbcUser, jdbcPassword, hfsStagingDir, credentials, redshiftTableDesc, (RedshiftScheme)jdbcScheme, sinkMode, keepDebugHdfsData, useDirectInsert );
    }



  private RedshiftTableDesc createTableDescFromProperties( Properties properties )
    {
    String tableName = properties.getProperty( PROTOCOL_TABLE_NAME );

    if( tableName == null || tableName.isEmpty() )
      throw new IllegalArgumentException( "no tablename given" );

    String separator = properties.getProperty( PROTOCOL_FIELD_SEPARATOR, DEFAULT_SEPARATOR );

    String[] columnNames = null;
    String columnNamesProperty = properties.getProperty( PROTOCOL_COLUMN_NAMES );
    if( columnNamesProperty != null && !columnNamesProperty.isEmpty() )
      columnNames = columnNamesProperty.split( separator );

    String[] columnDefs = null;
    String columnDefsProperty = properties.getProperty( PROTOCOL_COLUMN_DEFS );
    if( columnDefsProperty != null && !columnDefsProperty.isEmpty() )
      columnDefs = columnDefsProperty.split( separator );

    String distributionKey = properties.getProperty( FORMAT_DISTRIBUTION_KEY );

    String[] sortKeys = null;
    if( properties.containsKey( FORMAT_SORT_KEYS ) )
      sortKeys = properties.getProperty( FORMAT_SORT_KEYS ).split( DEFAULT_SEPARATOR );

    RedshiftTableDesc desc = new RedshiftTableDesc( tableName, columnNames, columnDefs, distributionKey, sortKeys );
    return desc;
    }


  /**
   * Helper method that tries to determine the AWS credentials. It first tries
   * the {@link Properties} passed in, next it checks for the environment
   * variables <code>AWS_ACCESS_KEY</code> and <code>AWS_SECRET_KEY</code>. If
   * none of the above contains the credentials, the method returns
   * {@link AWSCredentials}.
   *
   * @param properties a {@link Properties} object, which can contain the AWS
   *                   credentials.
   * @return an {@link AWSCredentials} installed.
   */
  private AWSCredentials determineAwsCredentials( Properties properties )
    {
    // try to determine the aws credentials, using the default unknown
    AWSCredentials awsCredentials = null;

    // first try the properties
    String awsAccessKey = properties.getProperty( PROTOCOL_AWS_ACCESS_KEY );
    String awsSecretKey = properties.getProperty( PROTOCOL_AWS_SECRET_KEY );

    if( !isPropertyNullOrEmpty( awsAccessKey ) && !isPropertyNullOrEmpty( awsSecretKey ) )
      awsCredentials = new AWSCredentials( awsAccessKey, awsSecretKey );

    // next try environment variables
    if( awsCredentials == null )
      {
      awsAccessKey = System.getenv( SYSTEM_AWS_ACCESS_KEY );
      awsSecretKey = System.getenv( SYSTEM_AWS_SECRET_KEY );
      if( !isPropertyNullOrEmpty( awsAccessKey ) && !isPropertyNullOrEmpty( awsSecretKey ) )
        awsCredentials = new AWSCredentials( awsAccessKey, awsSecretKey );
      }
    // return empty credentials on the assumption that the S3 target writeable without these
    return awsCredentials;
    }

  /**
   * Determines if a given string is <code>null</code> empty or only consists of
   * whitespace characters.
   *
   * @param string The string to check.
   * @return <code>true</code> if any of the above is true, otherwise
   *         <code>false</code>.
   */
  public static boolean isPropertyNullOrEmpty( String string )
    {
    return string == null || Strings.isNullOrEmpty( string.trim() );
    }

  public static Map<CopyOption, String> extractCopyOptions( Properties properties, String copyOptionsPrefix )
    {
    Map<CopyOption, String> copyOptions = new HashMap<CopyOption, String>();
    for( CopyOption curOption : CopyOption.values() )
      {
      String propConfName = copyOptionsPrefix + curOption.toString();
      if( properties.containsKey( propConfName ) )
        {
        String propValue = properties.get( propConfName ) != null ? properties.get( propConfName ).toString() : null;
        copyOptions.put( curOption, propValue );
        }
      }
    return copyOptions;
    }

  /** Enum of all the COPY options supported by the Redshift load command and information about how to covert them to SQL commands. */
  public static enum CopyOption
    {
      FIXEDWIDTH( "\'%s\'" ),
      DELIMITER( "\'%s\'" ),
      CSV( " QUOTE \'%s\' " ),
      ENCRYPTED,
      GZIP,
      LZOP,
      REMOVEQUOTES,
      EXPLICIT_IDS,
      ACCEPTINVCHARS( "\'%s\'" ),
      MAXERROR( "%s" ),
      DATEFORMAT( "\'%s\'" ),
      TIMEFORMAT( "\'%s\'" ),
      IGNOREHEADER( "%s" ),
      ACCEPTANYDATE,
      IGNOREBLANKLINES,
      TRUNCATECOLUMNS,
      FILLRECORD,
      TRIMBLANKS,
      NOLOAD,
      NULL( "\'%s\'" ),
      EMPTYASNULL,
      BLANKSASNULL,
      COMPROWS( "%s" ),
      COMPUPDATE( "%s" ),
      STATUPDATE( "%s" ),
      ESCAPE,
      ROUNDEC;

    private String formattableCommandString;

    CopyOption( String formattableCommandString )
      {
      this.formattableCommandString = formattableCommandString;
      }

    CopyOption()
      {
      this.formattableCommandString = "";
      }

    public String getArguments( String argument )
      {
      if( this.equals( CSV ) && argument == null )
        return " CSV ";

      if( formattableCommandString.length() == 0 || argument == null )
        return formattableCommandString;

      return String.format( formattableCommandString + " ", argument );
      }

    }


  }
