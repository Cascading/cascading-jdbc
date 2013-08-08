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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.Properties;

import org.junit.Test;

import cascading.tap.SinkMode;

/**
 * Tests for {@link JDBCFactory}.
 * 
 * */
public class JDBCFactoryTest
  {

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTapNoTableName()
      {
        String protocol = "jdbc";
        String identifier = "jdbc:some:stuf//database";
        JDBCScheme mockScheme = mock(JDBCScheme.class);

        JDBCFactory factory = new JDBCFactory();

        Properties props = new Properties();
        props.setProperty(JDBCFactory.PROTOCOL_FIELD_SEPARATOR, ":");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_DRIVER, "some.Driver");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_USER, "username");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_PASSWORD, "password");

        factory.createTap(protocol, mockScheme, identifier, SinkMode.REPLACE,
            props);
      }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTapNoColumns()
      {
        String protocol = "jdbc";
        String identifier = "jdbc:some:stuf//database";
        JDBCScheme mockScheme = mock(JDBCScheme.class);

        JDBCFactory factory = new JDBCFactory();

        Properties props = new Properties();
        props.setProperty(JDBCFactory.PROTOCOL_FIELD_SEPARATOR, ":");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_DRIVER, "some.Driver");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_USER, "username");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_PASSWORD, "password");

        props.setProperty(JDBCFactory.PROTOCOL_TABLE_NAME, "myTable");

        factory.createTap(protocol, mockScheme, identifier, SinkMode.REPLACE,
            props);
      }

    @Test(expected = IllegalArgumentException.class)
    public void testCreateTapNoColumnDefs()
      {
        String protocol = "jdbc";
        String identifier = "jdbc:some:stuf//database";
        JDBCScheme mockScheme = mock(JDBCScheme.class);

        JDBCFactory factory = new JDBCFactory();

        Properties props = new Properties();
        props.setProperty(JDBCFactory.PROTOCOL_FIELD_SEPARATOR, ":");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_DRIVER, "some.Driver");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_USER, "username");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_PASSWORD, "password");

        props.setProperty(JDBCFactory.PROTOCOL_TABLE_NAME, "myTable");
        props
            .setProperty(JDBCFactory.PROTOCOL_COLUMN_NAMES, "id:name:lastname");

        factory.createTap(protocol, mockScheme, identifier, SinkMode.UPDATE,
            props);
      }

    @Test()
    public void testCreateTapFullyWorking()
      {
        String protocol = "jdbc";
        String identifier = "jdbc:some:stuf//database";
        JDBCScheme mockScheme = mock(JDBCScheme.class);

        JDBCFactory factory = new JDBCFactory();

        Properties props = new Properties();
        props.setProperty(JDBCFactory.PROTOCOL_FIELD_SEPARATOR, ":");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_DRIVER, "some.Driver");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_USER, "username");
        props.setProperty(JDBCFactory.PROTOCOL_JDBC_PASSWORD, "password");

        props.setProperty(JDBCFactory.PROTOCOL_TABLE_NAME, "myTable");
        props
            .setProperty(JDBCFactory.PROTOCOL_COLUMN_NAMES, "id:name:lastname");

        props.setProperty(JDBCFactory.PROTOCOL_COLUMN_DEFS,
            "id int:name varchar(42):lastname varchar(23)");
        props.setProperty(JDBCFactory.PROTOCOL_PRIMARY_KEYS, "id");

        JDBCTap tap = (JDBCTap) factory.createTap(protocol, mockScheme,
            identifier, SinkMode.UPDATE, props);
        assertEquals(mockScheme, tap.getScheme());
        assertEquals("myTable", tap.getTableName());
        assertEquals(SinkMode.UPDATE, tap.getSinkMode());
        TableDesc tdesc = tap.tableDesc;

        assertEquals("myTable", tdesc.getTableName());
        assertArrayEquals(new String[] { "id", "name", "lastname" },
            tdesc.getColumnNames());
        assertArrayEquals(new String[] { "id int", "name varchar(42)",
            "lastname varchar(23)" }, tdesc.getColumnDefs());
        assertArrayEquals(new String[] { "id" }, tdesc.getPrimaryKeys());

      }
  }
