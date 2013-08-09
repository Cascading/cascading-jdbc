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
package cascading.provider.jdbc;

import java.util.Arrays;
import java.util.Properties;

import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.provider.jdbc.db.DBInputFormat;
import cascading.provider.jdbc.db.DBOutputFormat;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.google.common.collect.Lists;

/**
 * {@link JDBCFactory} is a factory class that can be used by the lingual
 * provider mechanism to create {@link JDBCScheme}s and {@link JDBCTap}s.
 * 
 * */
public class JDBCFactory
  {

    private static final Logger LOG = LoggerFactory
        .getLogger(JDBCFactory.class);

    public static final String DEFAULT_SEPARATOR = ":";

    public static final String PROTOCOL_JDBC_USER = "jdbcuser";
    public static final String PROTOCOL_JDBC_PASSWORD = "jdbcpassword";
    public static final String PROTOCOL_JDBC_DRIVER = "jdbcdriver";

    public static final String PROTOCOL_FIELD_SEPARATOR = "tabledesc.separator";
    public static final String PROTOCOL_TABLE_NAME = "tabledesc.tablename";
    public static final String PROTOCOL_COLUMN_NAMES = "tabledesc.columnnames";
    public static final String PROTOCOL_COLUMN_DEFS = "tabledesc.columndefs";
    public static final String PROTOCOL_PRIMARY_KEYS = "tabledesc.primarykeys";

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
     * @param protocol
     *          name of the protocol, only accepts "jdbc".
     * @param scheme
     *          a {@link JDBCScheme} instance.
     * @param identifier
     *          The identifier of the tap, which is assumed to be the jdbc URL.
     * @param mode
     *          a {@link SinkMode}. All are supported.
     * @param properties
     *          The Properties object containing the table description,
     *          optionally a jdbc user and a jdbc password.
     * @return a new {@link JDBCTap} instance.
     */
    @SuppressWarnings("rawtypes")
    public Tap createTap(String protocol, Scheme scheme, String identifier,
        SinkMode mode, Properties properties)
      {
        LOG.info("creating jdbc protocol with properties {} in mode {}", properties, mode);
        String jdbcUser = properties.getProperty(PROTOCOL_JDBC_USER);
        String jdbcPassword = properties.getProperty(PROTOCOL_JDBC_PASSWORD);
        String driver = properties.getProperty(PROTOCOL_JDBC_DRIVER);

        if (jdbcUser.isEmpty())
          jdbcUser = null;
        if (jdbcPassword.isEmpty())
          jdbcPassword = null;
        
        final TableDesc tableDesc = createTableDescFromProperties(properties);

        return new JDBCTap(identifier, jdbcUser, jdbcPassword, driver,
            tableDesc, (JDBCScheme) scheme, mode);

      }

    /**
     * Creates a new {@link JDBCScheme} instance for the given format, fields
     * and properties.
     * 
     * @param format
     *          The format of the scheme. This is JDBC driver dependent.
     * @param fields
     *          The fields to interact with.
     * @param properties
     *          The {@link Properties} object containing the necessary
     *          information to construct a {@link JDBCScheme}.
     * @return a new {@link JDBCScheme} instance.
     */
    @SuppressWarnings("rawtypes")
    public Scheme createScheme(String format, Fields fields,
        Properties properties)
      {

        LOG.info("creating {} format with properties {} and fields {}", format,
            properties, fields);

        String selectQuery = properties.getProperty(FORMAT_SELECT_QUERY);
        String countQuery = properties.getProperty(FORMAT_COUNT_QUERY);
        String separator = properties.getProperty(FORMAT_SEPARATOR,
            DEFAULT_SEPARATOR);
        long limit = -1;

        String limitProperty = properties.getProperty(FORMAT_LIMIT);
        if (limitProperty != null && !limitProperty.isEmpty())
          limit = Long.parseLong(limitProperty);

        String columnNamesProperty = properties.getProperty(FORMAT_COLUMNS);
        if (columnNamesProperty == null || columnNamesProperty == "")
          {
            LOG.error("no column names given");
            throw new IllegalArgumentException("no column names given");
          }

        String[] columNames = columnNamesProperty.split(separator);

        Boolean tableAlias = false;
        String tableAliasProperty = properties.getProperty(FORMAT_TABLE_ALIAS);
        if (tableAliasProperty != null)
          tableAlias = new Boolean(tableAliasProperty);

        if (selectQuery != null)
          {
            if (countQuery == null)
              throw new IllegalArgumentException(
                  "no count query for select query given");

            return new JDBCScheme(DBInputFormat.class, fields, columNames,
                selectQuery, countQuery, limit, tableAlias);
          }

        String conditions = properties.getProperty(FORMAT_CONDITIONS);

        String updateByProperty = properties.getProperty(FORMAT_UPDATE_BY);
        String[] updateBy = null;
        if (updateByProperty != null && !updateByProperty.isEmpty())
          updateBy = updateByProperty.split(separator);

        Fields updateByFields = null;
        if (updateByProperty != null && !updateByProperty.isEmpty())
          updateByFields = new Fields(updateBy);

        String[] orderBy = null;
        String orderByProperty = properties.getProperty(FORMAT_ORDER_BY);
        if (orderByProperty != null && !orderByProperty.isEmpty())
          orderBy = orderByProperty.split(separator);

        return new JDBCScheme(DBInputFormat.class, DBOutputFormat.class,
            fields, columNames, orderBy, conditions, limit, updateByFields,
            updateBy, tableAlias);

      }

    /**
     * Private helper method to extract values representing a {@link TableDesc}
     * instance from the properties passed to the createTap method.
     * 
     * @param properties
     *          A properties instance.
     * @return A {@link TableDesc} instance.
     * 
     */
    private TableDesc createTableDescFromProperties(Properties properties)
      {
        String tableName = properties.getProperty(PROTOCOL_TABLE_NAME);

        if (tableName == null || tableName.isEmpty())
          throw new IllegalArgumentException("no tablename given");

        String separator = properties.getProperty(PROTOCOL_FIELD_SEPARATOR,
            DEFAULT_SEPARATOR);

        String columnNamesProperty = properties
            .getProperty(PROTOCOL_COLUMN_NAMES);
        if (columnNamesProperty == null || columnNamesProperty.isEmpty())
          throw new IllegalArgumentException("no column names given");

        String[] columnNames = columnNamesProperty.split(separator);

        String columnDefsProperty = properties
            .getProperty(PROTOCOL_COLUMN_DEFS);
        if (columnDefsProperty == null || columnDefsProperty.isEmpty())
          throw new IllegalArgumentException("no column definitions given");

        String[] columnDefs = columnDefsProperty.split(separator);

        String primaryKeysProperty = properties
            .getProperty(PROTOCOL_PRIMARY_KEYS);

        String[] primaryKeys = null;

        if (primaryKeysProperty != null && !primaryKeysProperty.isEmpty())
          primaryKeys = primaryKeysProperty.split(separator);

        TableDesc desc = new TableDesc(tableName, columnNames, columnDefs,
            primaryKeys);
        return desc;
      }

  }
