package cascading.provider.jdbc;

import java.util.Properties;

import cascading.provider.jdbc.db.DBInputFormat;
import cascading.provider.jdbc.db.DBOutputFormat;
import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

public class JDBCFactory
  {

    public static final String PROTOCOL_JDBC_USER = "jdbcuser";
    public static final String PROTOCOL_JDBC_PASSWORD = "jdbcpassword";
    public static final String PROTOCOL_JDBC_DRIVER = "jdbcdriver";

    public static final String PROTOCOL_FIELD_SEPARATOR = "tabledesc.separator";
    public static final String PROTOCOL_TABLE_NAME = "tabledesc.tablename";
    public static final String PROTOCOL_COLUMN_NAMES = "tabledesc.columnnames";
    public static final String PROTOCOL_COLUMN_DEFS = "tabledesc.columndefs";
    public static final String PROTOCOL_PRIMARY_KEYS = "tabledesc.primarykeys";

    public static final String FORMAT_SEPARATOR = "separator";
    public static final String FORMAT_COMLUMNS = "columns";
    public static final String FORMAT_ORDER_BY = "orderBy";
    public static final String FORMAT_CONDITIONS = "conditions";
    public static final String FORMAT_LIMIT = "limit";
    public static final String FORMAT_UPDATEFIELDS_BY = "updateByFields";
    public static final String FORMAT_UPDATE_BY = "updateBy";
    public static final String FORMAT_TABLE_ALIAS = "tableAlias";

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
    public Tap createTap(String protocol, Scheme scheme, String identifier,
        SinkMode mode, Properties properties)
      {

        String jdbcUser = properties.getProperty(PROTOCOL_JDBC_USER);
        String jdbcPassword = properties.getProperty(PROTOCOL_JDBC_PASSWORD);
        String driver = properties.getProperty(PROTOCOL_JDBC_DRIVER);

        final TableDesc tableDesc = createTableDescFromProperties(properties);

        return new JDBCTap(identifier, jdbcUser, jdbcPassword, driver,
            tableDesc, (JDBCScheme) scheme, mode);

      }

    public Scheme createScheme()
      {
        // return new JDBCScheme(DBInputFormat.class, DBOutputFormat.class,
        // columnFields, columns, orderBy, conditions, limit, updateByFields,
        // updateBy, tableAlias)
        return null;
      }

    private TableDesc createTableDescFromProperties(Properties properties)
      {
        String tableName = properties.getProperty(PROTOCOL_TABLE_NAME);

        if (tableName == null)
          throw new IllegalArgumentException("no tablename given");

        String separator = properties.getProperty(PROTOCOL_FIELD_SEPARATOR);

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

        String[] primaryKeys = new String[] {};

        if (primaryKeysProperty != null)
          primaryKeys = primaryKeysProperty.split(separator);

        TableDesc desc = new TableDesc(tableName, columnNames, columnDefs,
            primaryKeys);
        return desc;
      }

  }
