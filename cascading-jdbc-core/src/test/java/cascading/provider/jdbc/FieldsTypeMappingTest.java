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

import static org.junit.Assert.assertEquals;

import java.sql.Date;
import java.sql.Timestamp;

import org.junit.Test;

public class FieldsTypeMappingTest
  {

  @Test
  public void testMappings()
    {
    assertEquals( "int not null", FieldsTypeMapping.sqltypeForClass( int.class ) );
    assertEquals( "int not null", FieldsTypeMapping.sqltypeForClass( long.class ) );
    assertEquals( "int", FieldsTypeMapping.sqltypeForClass( Integer.class ) );
    assertEquals( "int", FieldsTypeMapping.sqltypeForClass( Long.class ) );
    assertEquals( "varchar(256)", FieldsTypeMapping.sqltypeForClass( String.class ) );
    assertEquals( "timestamp", FieldsTypeMapping.sqltypeForClass( Timestamp.class ) );
    assertEquals( "date", FieldsTypeMapping.sqltypeForClass( Date.class ) );
    }
  
  @Test(expected=IllegalArgumentException.class)
  public void testUnknownClass()
    {
    FieldsTypeMapping.sqltypeForClass( boolean.class );
    }
  }
