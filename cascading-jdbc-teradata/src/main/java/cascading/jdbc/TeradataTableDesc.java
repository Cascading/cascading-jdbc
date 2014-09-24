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
import java.util.List;

import cascading.tuple.Fields;
import com.google.common.collect.Lists;

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
    }

  /**
   * {@inheritDoc}
   */
  @Override
  public void completeFromFields( Fields fields )
    {
    if( !hasRequiredTableInformation() )
      {
      List<String> names = Lists.newArrayList();
      List<String> defs = Lists.newArrayList();

      for( int i = 0; i < fields.size(); i++ )
        {
        Comparable<?> cmp = fields.get( i );
        names.add( cmp.toString() );
        Type internalType = TeradataInternalTypeMapping.findInternalType( fields.getType( i ) );
        defs.add( TeradataInternalTypeMapping.sqltypeForClass( internalType ) );
        }
      if( columnNames == null || columnNames.length == 0 ) columnNames = names.toArray( new String[ names.size() ] );
      if( columnDefs == null || columnDefs.length == 0 ) columnDefs = defs.toArray( new String[ defs.size() ] );

      // now it has to be complete and usable, if not bail out.
      if( !hasRequiredTableInformation() )
        throw new IllegalStateException( "could not derive TableDesc from given fields." );
      }
    }
  }
