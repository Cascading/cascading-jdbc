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

import cascading.lingual.type.SQLDateCoercibleType;
import cascading.tuple.Fields;
import org.junit.Test;

import static org.junit.Assert.*;

public class TableDescTest
  {

  @Test
  public void testHasRequiredTableInformation()
    {
    TableDesc desc = new TableDesc( "name" );
    assertFalse( desc.hasRequiredTableInformation() );

    desc = new TableDesc( "name", null, null, null, null );
    assertFalse( desc.hasRequiredTableInformation() );

    desc = new TableDesc( "name", new String[]{ "id" }, null, null, null );
    assertFalse( desc.hasRequiredTableInformation() );

    desc = new TableDesc( "name", new String[]{ "id" }, new String[]{ "int" }, null, "foo" );
    assertTrue( desc.hasRequiredTableInformation() );

    }

  @Test
  public void testCompleteFromFields()
    {
    TableDesc desc = new TableDesc( "name" );
    assertFalse( desc.hasRequiredTableInformation() );

    Fields fields = new Fields( "id", int.class );
    desc.completeFromFields( fields );

    assertTrue( desc.hasRequiredTableInformation() );

    assertArrayEquals( new String[]{ "id" }, desc.getColumnNames() );

    assertArrayEquals( new String[]{ "int not null" }, desc.getColumnDefs() );
    }

  @Test
  public void testCompleteFromFieldsWithCoercibleType()
    {
    TableDesc desc = new TableDesc( "name" );
    assertFalse( desc.hasRequiredTableInformation() );

    Fields fields = new Fields( "creation_date", new SQLDateCoercibleType() );
    desc.completeFromFields( fields );

    assertTrue( desc.hasRequiredTableInformation() );

    assertArrayEquals( new String[]{ "creation_date" }, desc.getColumnNames() );

    assertArrayEquals( new String[]{ "date" }, desc.getColumnDefs() );

    }

  @Test(expected = IllegalArgumentException.class)
  public void testCompleteFromFieldsMissingType()
    {
    TableDesc desc = new TableDesc( "name" );
    assertFalse( desc.hasRequiredTableInformation() );

    Fields fields = new Fields( "id" );
    desc.completeFromFields( fields );
    }

  @Test(expected = IllegalStateException.class)
  public void testCompleteFromFieldsWithUnknownFields()
    {
    TableDesc desc = new TableDesc( "name" );
    assertFalse( desc.hasRequiredTableInformation() );

    Fields fields = Fields.UNKNOWN;
    desc.completeFromFields( fields );
    }

  }
