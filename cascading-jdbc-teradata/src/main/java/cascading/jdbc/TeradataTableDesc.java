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

package cascading.jdbc;

import java.io.Serializable;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;

import cascading.tuple.Fields;

/**
 * Class TeradataTableDesc extends TableDesc  which describes a SQL based table,
 * this description is used by the
 * {@link JDBCTap} when creating a missing table and by the JDBCScheme, for the
 * correct type coercion.
 * <p/>
 * This class is used to override completeFromFields to use TeradataInternalMapping.java
 *
 * @see JDBCTap
 * @see JDBCScheme
 */
public class TeradataTableDesc extends TableDesc implements Serializable
  {
  private static final long serialVersionUID = 5009899098019404131L;

  /**
   * Field columnNames
   */
  String[] columnNames;
  /**
   * Field primaryKeys
   */
  String[] primaryKeys;

  /**
   * Constructor TeradataTableDesc creates a new TeradataTableDesc instance.
   *
   * @param tableName   of type String
   * @param columnNames of type String[]
   * @param columnDefs  of type String[]
   * @param primaryKeys of type String
   */
  public TeradataTableDesc( String tableName, String[] columnNames, String[] columnDefs, String[] primaryKeys )
    {
    super( tableName, columnNames, columnDefs, primaryKeys );
    this.columnNames = columnNames;
    this.primaryKeys = primaryKeys;
    }

  /**
   * {@inheritDoc}
   */
  @Override
  public void completeFromFields( Fields fields )
    {
    if( !hasRequiredTableInformation() )
      {
      List<String> names = new ArrayList<String>();
      List<String> defs = new ArrayList<String>();

      for( int i = 0; i < fields.size(); i++ )
        {
        Comparable<?> cmp = fields.get( i );
        names.add( cmp.toString() );
        Type internalType = InternalTypeMapping.findInternalType( fields.getType( i ) );
        String type = InternalTypeMapping.sqltypeForClass( internalType );
        defs.add( type );
        }
      if( columnNames == null || columnNames.length == 0 )
        columnNames = names.toArray( new String[ names.size() ] );
      if( columnDefs == null || columnDefs.length == 0 )
        columnDefs = defs.toArray( new String[ defs.size() ] );

      for( int i = 0; i < columnNames.length; i++ )
        {
        if( Arrays.asList( primaryKeys ).contains( columnNames[ i ] ) )
          {
          if( columnDefs[ i ].equalsIgnoreCase( "varchar(256)" ) )
            columnDefs[ i ] = "varchar(256) not null";
          }
        }

      // now it has to be complete and usable, if not bail out.
      if( !hasRequiredTableInformation() )
        throw new IllegalStateException( "could not derive TableDesc from given fields." );
      }
    }
  }
