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
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.util.Util;
import com.google.common.collect.Lists;

/**
 * Class TableDesc describes a SQL based table, this description is used by the
 * {@link JDBCTap} when creating a missing table and by the JDBCScheme, for the
 * correct type coercion.
 *
 * @see JDBCTap
 * @see JDBCScheme
 */
public class TableDesc implements Serializable
  {

  private static final long serialVersionUID = 5009899098019404131L;

  /** Field tableName */
  String tableName;
  /** Field columnNames */
  String[] columnNames;
  /** Field columnDefs */
  String[] columnDefs;
  /** Field primaryKeys */
  String[] primaryKeys;

  String tableExistsQuery;

  private Map<Comparable<?>, Type> internalColumnTypeMapping;

  /**
   * Constructor TableDesc creates a new TableDesc instance.
   *
   * @param tableName of type String
   */
  public TableDesc( String tableName )
    {
    this.tableName = tableName;
    }

  /**
   * Constructor TableDesc creates a new TableDesc instance.
   *
   * @param tableName of type String
   * @param columnNames of type String[]
   * @param columnDefs of type String[]
   * @param primaryKeys of type String
   *
   */
  public TableDesc( String tableName, String[] columnNames, String[] columnDefs, String[] primaryKeys, String tableExistsQuery )
    {
    this.tableName = tableName;
    this.columnNames = columnNames;
    this.columnDefs = columnDefs;
    this.primaryKeys = primaryKeys;
    this.tableExistsQuery = tableExistsQuery;
    }

  public String getTableName()
    {
    return tableName;
    }

  public String[] getColumnNames()
    {
    return columnNames;
    }

  public String[] getColumnDefs()
    {
    return columnDefs;
    }

  public String[] getPrimaryKeys()
    {
    return primaryKeys;
    }

  /**
   * Method getTableCreateStatement returns the tableCreateStatement of this
   * TableDesc object.
   *
   * @return the tableCreateStatement (type String) of this TableDesc object.
   */
  public String getCreateTableStatement()
    {
    List<String> createTableStatement = new ArrayList<String>();

    createTableStatement = addCreateTableBodyTo( createTableStatement );

    return String.format( getCreateTableFormat(), tableName, Util.join( createTableStatement, ", " ) );
    }

  protected List<String> addCreateTableBodyTo( List<String> createTableStatement )
    {
    createTableStatement = addDefinitionsTo( createTableStatement );
    createTableStatement = addPrimaryKeyTo( createTableStatement );

    return createTableStatement;
    }

  protected String getCreateTableFormat()
    {
    return "CREATE TABLE %s ( %s )";
    }

  protected List<String> addDefinitionsTo( List<String> createTableStatement )
    {
    for( int i = 0; i < columnNames.length; i++ )
      {
      String columnName = columnNames[ i ];
      String columnDef = columnDefs[ i ];

      createTableStatement.add( columnName + " " + columnDef );
      }

    return createTableStatement;
    }

  protected List<String> addPrimaryKeyTo( List<String> createTableStatement )
    {
    if( hasPrimaryKey() )
      createTableStatement.add( String.format( "PRIMARY KEY( %s )", Util.join( primaryKeys, ", " ) ) );

    return createTableStatement;
    }

  /**
   * Method getTableDropStatement returns the tableDropStatement of this
   * TableDesc object.
   *
   * @return the tableDropStatement (type String) of this TableDesc object.
   */
  public String getTableDropStatement()
    {
    return String.format( getDropTableFormat(), tableName );
    }

  protected String getDropTableFormat()
    {
    return "DROP TABLE %s";
    }

  /**
   * Method getTableExistsQuery returns the tableExistsQuery of this TableDesc
   * object.
   *
   * @return the tableExistsQuery (type String) of this TableDesc object.
   */
  public String getTableExistsQuery()
    {
    if( canQueryExistence() )
      {
      if( !Utils.isNullOrEmpty( tableExistsQuery ) )
        return String.format( tableExistsQuery, tableName );
      else
        return String.format( JDBCFactory.DEFAULT_TABLE_EXISTS_QUERY, tableName );
      }
    else
      {
      return String.format( JDBCFactory.DEFAULT_TABLE_EXISTS_QUERY, tableName );
      }
    }

  public boolean canQueryExistence()
    {
    if( Utils.isNullOrEmpty( tableExistsQuery ) )
      return true; // default to assuming we can query the table
    else
      return ( !tableExistsQuery.equals( JDBCFactory.TABLE_EXISTS_UNSUPPORTED ) );
    }

  private boolean hasPrimaryKey()
    {
    return primaryKeys != null && primaryKeys.length != 0;
    }

  /**
   * Determines if the instance has a useful tablename, columns and column
   * descriptions set. Useful means, that they are non-null and not empty and
   * for each column, there is a type definition.
   *
   * @return Returns <code>true</code> if all requirements are met, otherwise
   *         <code>false</code>.
   * */
  public boolean hasRequiredTableInformation()
    {
    return tableName != null && !tableName.isEmpty() && columnNames != null && columnNames.length > 0 && columnDefs != null
        && columnNames.length > 0 && columnDefs.length == columnNames.length ;
    }

  /**
   * This method can be used to fill in the required column names and
   * descriptions from a Fields instance. This can be useful, when the types can
   * only be determined after a flow has been started. This mechanism can be
   * used to simplify the usage of the {@link JDBCTap} and {@link JDBCScheme} as
   * a provider. The method may throw an {@link IllegalStateException} if the
   * fields are insufficient to determine the correct types.
   *
   *
   * @param fields The {@link Fields} instance to derive the table structure
   *          from.
   *
   * @throws IllegalArgumentException In case the instance is still incomplete,
   *           after trying to determine the table structure fromt he given
   *           fields.
   */
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
        Type internalType = InternalTypeMapping.findInternalType( fields.getType( i ) );
        defs.add( InternalTypeMapping.sqltypeForClass( internalType ) );
        }
      if( columnNames == null )
        columnNames = names.toArray( new String[names.size()] );
      if( columnDefs == null )
        columnDefs = defs.toArray( new String[defs.size()] );

      // now it has to be complete and usable, if not bail out.
      if( !hasRequiredTableInformation() )
        throw new IllegalStateException( "could not derive TableDesc from given fields." );
      }
    }

  @Override
  public String toString()
    {
    return "TableDesc{" + "tableName='" + tableName + '\'' + ", columnNames="
        + ( columnNames == null ? null : Arrays.asList( columnNames ) ) + ", columnDefs="
        + ( columnDefs == null ? null : Arrays.asList( columnDefs ) ) + ", primaryKeys="
        + ( primaryKeys == null ? null : Arrays.asList( primaryKeys ) ) + '}';
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( ! ( object instanceof TableDesc ) )
      return false;

    TableDesc tableDesc = (TableDesc) object;

    if( !Arrays.equals( columnDefs, tableDesc.columnDefs ) )
      return false;
    if( !Arrays.equals( columnNames, tableDesc.columnNames ) )
      return false;
    if( !Arrays.equals( primaryKeys, tableDesc.primaryKeys ) )
      return false;
    if( tableName != null ? !tableName.equals( tableDesc.tableName ) : tableDesc.tableName != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = tableName != null ? tableName.hashCode() : 0;
    result = 31 * result + ( columnNames != null ? Arrays.hashCode( columnNames ) : 0 );
    result = 31 * result + ( columnDefs != null ? Arrays.hashCode( columnDefs ) : 0 );
    result = 31 * result + ( primaryKeys != null ? Arrays.hashCode( primaryKeys ) : 0 );
    return result;
    }
  }
