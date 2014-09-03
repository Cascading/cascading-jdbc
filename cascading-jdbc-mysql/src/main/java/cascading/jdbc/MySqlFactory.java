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

import java.util.Properties;

import cascading.jdbc.db.DBOutputFormat;
import cascading.jdbc.db.DBInputFormat;
import cascading.jdbc.db.MySqlDBOutputFormat;
import cascading.jdbc.db.MySqlDBInputFormat;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;

/**
 * Subclass of JDBCFactory with mysql specific behaviour.
 */
public class MySqlFactory extends JDBCFactory
  {
  public static final String PROTOCOL_REPLACE_ON_INSERT = "replaceoninsert";

  @Override
  protected Class<? extends DBOutputFormat> getOutputFormClass()
    {
    return MySqlDBOutputFormat.class;
    }

  @Override
  protected Class<? extends DBInputFormat> getInputFormatClass()
  {
      return MySqlDBInputFormat.class;
  }

  protected Scheme createUpdatableScheme( Fields fields, long limit, String[] columnNames, Boolean tableAlias, String conditions,
                                          String[] updateBy, Fields updateByFields, String[] orderBy, Properties properties )
    {
    boolean replaceOnInsert = false;
    String replaceOnInsertProperty = properties.getProperty( PROTOCOL_REPLACE_ON_INSERT );
    if ( replaceOnInsertProperty != null && !replaceOnInsertProperty.isEmpty() )
      replaceOnInsert = Boolean.parseBoolean( replaceOnInsertProperty );

    return new MySqlScheme( getInputFormatClass(), getOutputFormClass(), fields, columnNames, orderBy, conditions, limit, updateByFields,
      updateBy, tableAlias, replaceOnInsert );
    }

  protected Scheme createScheme( Fields fields, String selectQuery, String countQuery, long limit, String[] columnNames, Boolean tableAlias )
    {
    return new MySqlScheme( getInputFormatClass(), fields, columnNames, selectQuery, countQuery, limit, tableAlias);
    }

  }
