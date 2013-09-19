__NOTE:__ This is work in progress and can change any time. Please do not
use it in production applications yet.

# cascading-jdbc

A set cascading (2.2) Taps and Schemes which interact with RDBMS systems via JDBC. The
project consists of a generic part `cascading-jdbc-core` and database specific
sub-projects. The database specific projects have dependencies to their
respective JDBC drivers and run tests against those systems during build. 

Currently three relational databases are supported in the build:

* [derby](http://db.apache.org/derby/)
* [h2](http://www.h2database.com/html/main.html)
* [mysql](http://www.mysql.com/)
* [postgres](http://www.postgresql.org/)

This code is based on previous work:

* https://github.com/Cascading/maple
* https://github.com/cwensel/cascading.jdbc

Both are based on code coming from [apache hadoop](http://hadoop.apache.org).

# Building and Testing

Building all jars is done with a simple `gradle build`. This produces "normal"
jar files, to be used within cascading applications as well as "fat" provider jars,
that can be used within [lingual](http://docs.cascading.org/lingual/1.0/).

Database systems like `mysql` require an external database server. In order to
be able to test with an external server, the build uses system properties, which
can be given on the command line. 

Due to this the sub-project for `mysql` is only enabled, if the connnection
information is given to gradle like this:

    > gradle build -Dcascading.jdbc.url.mysql="jdbc:mysql://some-host/somedb?user=someuser&password=somepw"

The same applies to the postgres sub-project, which will only work, if you run
the build like this:


    > gradle build -Dcascading.jdbc.url.postgres='jdbc:postgresql://192.168.33.10/cascading?user=postgres&password=password'

Debian based systems turn on SSL encryption of the databases by default, but are
using self signed certificates. If you want to connect to such a database, you
have to add `sslfactory=org.postgresql.ssl.NonValidatingFactory` to the JDBC
url.


The database specific test are implemented as an abstract class in the core
project `cascading.provider.jdbc.JDBCTestingBase`. Each project has a sub-class
setting driver specific things. Some might perform more sophisticated setups,
like starting and stopping an in-process server during `setUp()` and
`tearDown()`, respectively.

You can install the jars into a local maven repository with 

    > gralde install

or you can use the ones deployed to [http://conjars.org](conjars).

# Usage

## In Cascading applications

With the `JDBCTap` and `JDBCScheme` you can read from database tables, create
new tables and write to them, write into existing tables and, update existing
tables. All modes of operation are used in the test base class
`cascading.provider.jdbc.JDBCTestingBase` and should be self explanatory. 

Please note that updating a database table is supported, but not recommended for
long running jobs. It is considered a convenience during testing/development. 

Typical work loads write into a new table, which is afterwards made available to
other parts of your system via a post process. You can achieve this by using the
`onCompleted(Flow flow)` method in a class implementing the
[`cascading.flow.FlowListener`](http://docs.cascading.org/cascading/2.1/javadoc/cascading/flow/FlowListener.html)
interface. In case something goes wrong during the execution of your Flow, you
can clean up your database table in the `onThrowable(Flow flow)` method of your
`FlowListener` implementation.

## In Lingual

__NOTE__: The JDBC providers can only be used on the hadoop platform. The local
platform is not supported.

This assumes, that you have followed the lingual tutorial, esp. the part, where
a provider is used to write directly into a memcached server. To accomplish the
same, but with a derby database, you can do the following. First run `gradle
build` in this project. Next setup your lingual catalog with the derby provider:

    # only hadoop platform is supported
    > export LINGUAL_PLATFORM=hadoop
    > lingual catalog --provider -add /path/to/cascading-jdbc-derby-2.2.0-wip-dev-provider.jar

This will register the provider `derby` for the `hadoop` platform. The provider
supports one protocol (`jdbc`) and one format (`derby`).

Next we can add the `working` schema, the `titles` stereotype and the
`title_counts` table.

    > lingual catalog --schema working --add
    > lingual catalog --schema working --stereotype titles -add --columns TITLE,CNT --types string,int
    > lingual catalog --schema working --format derby --add --properties columnnames=title:cnt --provider derby

Next we set the protocol properties for `jdbc` in the `derby` provider. The
first line describes the table, we are operating on to the underlying JDBCTap.
The table has two columns `title` and `cnt`, which are of type `varchar(100)`
and `int`. The command line interface uses `:` as a separator for properties,
which contain multiple values.

    > lingual catalog --schema working --protocol jdbc --add "--properties=tabledesc.tablename=title_counts,tabledesc.columnnames=title:cnt,tabledesc.columndefs=varchar(100) not null:int not null" --provider derby

Finally we tell the derby provider, where it can find the derby server. The
`create=true` is optional. If the database exist already, you can omit it.

    > lingual catalog --schema working --table title_counts --stereotype titles -add "jdbc:derby://localhost:1527/mydb;create=true" --protocol jdbc  --format derby 

Now the table `title_counts` is ready to be used from within lingual as a sink.
You can now run a query over the employe data from the lingual tutorial on hdfs
directly into your derby server like this:

    > lingual shell
    (lingual shell)  insert into "working"."title_counts" select title, count( title ) as cnt from employees.titles group by title;
    +-----------+
    | ROWCOUNT  |
    +-----------+
    | 7         |
    +-----------+
    1 row selected (9,581 seconds)

The provider can not only be used as a sink, but also as a source, meaning you
can investigat the data, that was just written into the derby table directly
from the lingual shell:

    > (lingual shell) select * from "working"."title_counts"
    +---------------------+---------+
    |        TITLE        |   CNT   |
    +---------------------+---------+
    | Assistant Engineer  | 15128   |
    | Engineer            | 115003  |
    | Manager             | 24      |
    | Senior Engineer     | 97749   |
    | Senior Staff        | 92853   |
    | Staff               | 107391  |
    | Technique Leader    | 15159   |
    +---------------------+---------+
    7 rows selected (0,172 seconds)


# Extending

Adding a new JDBC system is straight forward. Create a new sub-project and add
it to `settings.gradle`. Include the JDBC driver of your RDBMS in the
`build.gradle` file of your sub-project and create a `TestCase` subclassing the
`JDBCTestingBase` explained above. All you have to do, is setting the driver
class and JDBC URL for your database. For an example see
[H2Test](https://github.com/Cascading/cascading-jdbc/blob/wip-2.2/cascading-jdbc-mysql/src/test/java/cascading/jdbc/MysqlTest.java).


## Provider mechanism

If you want to use your driver as a provider within lingual, you have to include
a `provider.properties` in the `src/main/resources/cascading/bind` directory of
your project. The name of the provider should match your database, the protocol
should be set to `jdbc` and the format to the type of database you are using.

Below is an example from the `derby` subproject:

    # default name of provider
    cascading.bind.provider.names=derby
    cascading.bind.provider.derby.platforms=hadoop

    cascading.bind.provider.derby.factory.classname=cascading.jdbc.JDBCFactory

    # define protocols differentiated by properties
    cascading.bind.provider.derby.protocol.names=jdbc
    cascading.bind.provider.derby.protocol.jdbc.schemes=derby
    cascading.bind.provider.derby.protocol.jdbc.jdbcdriver=org.apache.derby.jdbc.ClientDriver
    cascading.bind.provider.derby.protocol.jdbc.tabledescseparator=:
    cascading.bind.provider.derby.protocol.jdbc.jdbcuser=
    cascading.bind.provider.derby.protocol.jdbc.jdbcpassword=
    cascading.bind.provider.derby.protocol.jdbc.tabledesc.tablename=
    cascading.bind.provider.derby.protocol.jdbc.tabledesc.columnnames=
    cascading.bind.provider.derby.protocol.jdbc.tabledesc.columndefs=
    cascading.bind.provider.derby.protocol.jdbc.tabledesc.primarykeys=

    # define formats differentiated by properties
    cascading.bind.provider.derby.format.names=derby
    cascading.bind.provider.derby.format.derby.protocols=jdbc
    cascading.bind.provider.derby.format.derby.columnfields=
    cascading.bind.provider.derby.format.derby.separator=:
    cascading.bind.provider.derby.format.derby.columnnames=
    cascading.bind.provider.derby.format.derby.orderBy=
    cascading.bind.provider.derby.format.derby.conditions=
    cascading.bind.provider.derby.format.derby.limit=
    cascading.bind.provider.derby.format.derby.updateBy=
    cascading.bind.provider.derby.format.derby.tableAlias=
    cascading.bind.provider.derby.format.derby.selectquery=
    cascading.bind.provider.derby.format.derby.countquery=

For more information on the `provider.properties` file, see the [lingual
documentation](http://docs.cascading.org/lingual/1.0/#_creating_a_data_provider).

# Licsense

All code, unless otherwise noted, is licensed under the Apache Software License
version 2.
