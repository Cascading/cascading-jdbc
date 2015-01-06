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

import java.util.Properties;

import org.apache.hadoop.mapred.InputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.jdbc.db.DBInputFormat;
import cascading.jdbc.db.DBOutputFormat;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

/**
 * {@link JDBCFactory} is a factory class that can be used by the lingual
 * provider mechanism to create {@link JDBCScheme}s and {@link JDBCTap}s.
 *
 * */
public class JDBCFactory
  {

  private static final Logger LOG = LoggerFactory.getLogger( JDBCFactory.class );

  public static final String DEFAULT_SEPARATOR = ":";

  public static final String PROTOCOL_JDBC_USER = "jdbcuser";
  public static final String PROTOCOL_JDBC_PASSWORD = "jdbcpassword";
  public static final String PROTOCOL_JDBC_DRIVER = "jdbcdriver";
  public static final String DEFAULT_TABLE_EXISTS_QUERY =  "select 1 from %s where 1 = 0";
  public static final String TABLE_EXISTS_UNSUPPORTED = "__TABLE_EXISTS_QUERY_UNSUPPORTED__";

  public static final String PROTOCOL_FIELD_SEPARATOR = "tabledesc.separator";
  public static final String PROTOCOL_TABLE_NAME = "tabledesc.tablename";
  public static final String PROTOCOL_COLUMN_NAMES = "tabledesc.columnnames";
  public static final String PROTOCOL_COLUMN_DEFS = "tabledesc.columndefs";
  public static final String PROTOCOL_PRIMARY_KEYS = "tabledesc.primarykeys";
  public static final String PROTOCOL_SINK_MODE = "sinkmode";

  public static final String FORMAT_SEPARATOR = "separator";
  public static final String FORMAT_COLUMNS = "columnnames";
  public static final String FORMAT_ORDER_BY = "orderBy";
  public static final String FORMAT_CONDITIONS = "conditions";
  public static final String FORMAT_LIMIT = "limit";
  public static final String FORMAT_UPDATE_BY = "updateBy";
  public static final String FORMAT_TABLE_ALIAS = "tableAlias";

  public static final String FORMAT_SELECT_QUERY = "selectQuery";
  public static final String FORMAT_COUNT_QUERY = "countQuery";

  /**
   * Creates a new Tap for the given arguments.
   *
   * @param protocol name of the protocol, only accepts "jdbc".
   * @param scheme a {@link JDBCScheme} instance.
   * @param identifier The identifier of the tap, which is assumed to be the
   *          jdbc URL.
   * @param mode a {@link SinkMode}. All are supported.
   * @param properties The Properties object containing the table description,
   *          optionally a jdbc user and a jdbc password.
   * @return a new {@link JDBCTap} instance.
   */
  @SuppressWarnings("rawtypes")
  public Tap createTap( String protocol, Scheme scheme, String identifier, SinkMode mode, Properties properties )
    {
    LOG.info( "creating jdbc protocol with properties {} in mode {}", properties, mode );

    String driver = properties.getProperty( PROTOCOL_JDBC_DRIVER );

    String jdbcUserProperty = properties.getProperty( PROTOCOL_JDBC_USER );
    String jdbcPasswordProperty = properties.getProperty( PROTOCOL_JDBC_PASSWORD );

    String jdbcUser = null;
    if( jdbcUserProperty != null && !jdbcUserProperty.isEmpty() )
      jdbcUser = jdbcUserProperty;

    String jdbcPassword = null;
    if( jdbcPasswordProperty != null && !jdbcPasswordProperty.isEmpty() )
      jdbcPassword = jdbcPasswordProperty;

    final TableDesc tableDesc = createTableDescFromProperties( properties );

    JDBCScheme jdbcScheme = (JDBCScheme) scheme;

    /*
     * it is possible, that the schema information given via properties is
     * incomplete and therefore, we derive it from the given fields. We can only
     * do that, if we actually get meaningful fields. There is a second place,
     * where this happens, which is the presentSinkFields method of the
     * JDBCScheme.
     */
    Fields sinkFields = jdbcScheme.getSinkFields();
    if( !tableDesc.hasRequiredTableInformation() && sinkFields != Fields.UNKNOWN && sinkFields != Fields.ALL && sinkFields != null
        && sinkFields.getTypes() != null )
      {
      LOG.debug( "tabledesc information incomplete, falling back to sink-fields {}", jdbcScheme.getSinkFields() );
      tableDesc.completeFromFields( jdbcScheme.getSinkFields() );
      ( (JDBCScheme) scheme ).setColumns( tableDesc.getColumnNames() );
      }

    // users can overwrite the sink mode.
    String sinkModeProperty = properties.getProperty( PROTOCOL_SINK_MODE );
    SinkMode userMode = mode;
    if( sinkModeProperty != null && !sinkModeProperty.isEmpty() )
      userMode = SinkMode.valueOf( sinkModeProperty );

    return new JDBCTap( identifier, jdbcUser, jdbcPassword, driver, tableDesc, jdbcScheme, userMode );

    }

  /**
   * Creates a new {@link JDBCScheme} instance for the given format, fields and
   * properties.
   *
   * @param format The format of the scheme. This is JDBC driver dependent.
   * @param fields The fields to interact with.
   * @param properties The {@link Properties} object containing the necessary
   *          information to construct a {@link JDBCScheme}.
   * @return a new {@link JDBCScheme} instance.
   */
  @SuppressWarnings("rawtypes")
  public Scheme createScheme( String format, Fields fields, Properties properties )
    {
    LOG.info( "creating {} format with properties {} and fields {}", format, properties, fields );

    String selectQuery = properties.getProperty( FORMAT_SELECT_QUERY );
    String countQuery = properties.getProperty( FORMAT_COUNT_QUERY );
    String separator = properties.getProperty( FORMAT_SEPARATOR, DEFAULT_SEPARATOR );
    long limit = -1;

    String limitProperty = properties.getProperty( FORMAT_LIMIT );
    if( limitProperty != null && !limitProperty.isEmpty() )
      limit = Long.parseLong( limitProperty );

    String[] columnNames = getColumnNames(fields, properties, separator);

    Boolean tableAlias = getTableAlias(properties);

    if( selectQuery != null )
      {
      if( countQuery == null )
        throw new IllegalArgumentException( "no count query for select query given" );

      return createScheme( fields, selectQuery, countQuery, limit, columnNames, tableAlias );
      }

    String conditions = properties.getProperty( FORMAT_CONDITIONS );

    String updateByProperty = properties.getProperty( FORMAT_UPDATE_BY );
    String[] updateBy = null;
    if( updateByProperty != null && !updateByProperty.isEmpty() )
      updateBy = updateByProperty.split( separator );

    Fields updateByFields = null;
    if( updateByProperty != null && !updateByProperty.isEmpty() )
      updateByFields = new Fields( updateBy );

    String[] orderBy = null;
    String orderByProperty = properties.getProperty( FORMAT_ORDER_BY );
    if( orderByProperty != null && !orderByProperty.isEmpty() )
      orderBy = orderByProperty.split( separator );

    return createUpdatableScheme( fields, limit, columnNames, tableAlias, conditions, updateBy, updateByFields, orderBy );

    }

  protected Scheme createUpdatableScheme( Fields fields, long limit, String[] columnNames, Boolean tableAlias, String conditions,
                                          String[] updateBy, Fields updateByFields, String[] orderBy, Properties properties )
    {
    return new JDBCScheme( getInputFormatClass(), getOutputFormClass(), fields, columnNames, orderBy, conditions, limit, updateByFields,
      updateBy, tableAlias );
    }

  protected Scheme createUpdatableScheme( Fields fields, long limit, String[] columnNames, Boolean tableAlias, String conditions,
      String[] updateBy, Fields updateByFields, String[] orderBy )
    {
    return createUpdatableScheme( fields, limit, columnNames, tableAlias, conditions, updateBy, updateByFields, orderBy, new Properties() );
    }

  protected Scheme createScheme( Fields fields, String selectQuery, String countQuery, long limit, String[] columnNames, boolean tableAlias )
    {
    return new JDBCScheme( getInputFormatClass(), fields, columnNames, selectQuery, countQuery, limit, tableAlias );
    }

  /**
   * Private helper method to extract values representing a {@link TableDesc}
   * instance from the properties passed to the createTap method.
   *
   * @param properties A properties instance.
   * @return A {@link TableDesc} instance.
   *
   */
  protected TableDesc createTableDescFromProperties( Properties properties )
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

    String primaryKeysProperty = properties.getProperty( PROTOCOL_PRIMARY_KEYS );

    String[] primaryKeys = null;

    if( primaryKeysProperty != null && !primaryKeysProperty.isEmpty() )
      primaryKeys = primaryKeysProperty.split( separator );

    TableDesc desc = new TableDesc( tableName, columnNames, columnDefs, primaryKeys );
    return desc;
    }

  /**
   * Returns {@link DBInputFormat} class. This can be overwritten in subclasses, if they
   * have a custom {@link DBInputFormat}.
   *
   * @return the {@link InputFormat} to use.
   * */
  protected Class<? extends DBInputFormat> getInputFormatClass()
    {
    return DBInputFormat.class;
    }

  /**
   * Returns {@link DBOutputFormat} class. This can be overwritten in subclasses, if they
   * have a custom {@link DBInputFormat}.
   *
   * @return the {@link InputFormat} to use.
   * */
  protected Class<? extends DBOutputFormat> getOutputFormClass()
    {
    return DBOutputFormat.class;
    }

  protected String[] getColumnNames( Fields fields, Properties properties, String separator )
    {
    String[] columNames = null;
    String columnNamesProperty = properties.getProperty( FORMAT_COLUMNS );
    if( columnNamesProperty != null && !columnNamesProperty.isEmpty() )
      columNames = columnNamesProperty.split( separator );
    else if( fields != null )
      {
      columNames = new String[ fields.size() ];
      for( int i = 0; i < fields.size(); i++ )
        {
        Comparable<?> cmp = fields.get( i );
        columNames[ i ] = cmp.toString();
        }
      }
    return columNames;
    }

  protected Boolean getTableAlias( Properties properties )
    {
    Boolean tableAlias = false;
    String tableAliasProperty = properties.getProperty( FORMAT_TABLE_ALIAS );
    if( tableAliasProperty != null )
      tableAlias = new Boolean( tableAliasProperty );

    return tableAlias;
    }

  }
