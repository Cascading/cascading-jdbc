# cascading-jdbc

A set of Cascading (version 3.0 and above) Taps and Schemes which interact with RDBMS systems via JDBC. The project
consists of a generic part `cascading-jdbc-core` and database specific sub-projects. The database specific projects have
dependencies to their respective JDBC drivers and run tests against those systems during build.

Currently seven relational databases are supported in the build:

* [derby](http://db.apache.org/derby/)
* [h2](http://www.h2database.com/html/main.html)
* [mysql](http://www.mysql.com/)
* [postgresql](http://www.postgresql.org/)
* [Amazon Redshift](http://aws.amazon.com/redshift/)
* [oracle](http://www.oracle.com/technetwork/database/enterprise-edition/overview/index.html) (see below)
* [teradata](https://downloads.teradata.com/download/connectivity/jdbc-driver) (see below)

This code is based on previous work:

* https://github.com/Cascading/maple
* https://github.com/cwensel/cascading.jdbc

Both are based on code coming from [apache hadoop](http://hadoop.apache.org).

# Building and Testing

Building all jars is done with a simple `gradle build`. This produces "normal" jar files,
to be used within cascading applications as well as "fat" provider jars, that can be used within
[lingual](http://docs.cascading.org/lingual/1.1/).

The database specific test are implemented as an abstract class in the core project
`cascading.provider.jdbc.JDBCTestingBase`. Each project has a sub-class setting driver specific things. Some might
perform more sophisticated setups, like starting and stopping an in-process server during `setUp()` and `tearDown()`,
respectively.

You can install the jars into a local maven repository with

    > gradle install

or you can use the ones deployed to [conjars](http://conjars.org).


## mysql and postgresql

Database systems like `mysql` require an external database server. In order to be able to test with an external server,
the build uses system properties, which can be given on the command line.

Due to this the sub-projects for `mysql` and `postgresql` are only enabled, if the connnection information is given to
gradle like this:

    > gradle build -Dcascading.jdbc.url.mysql="jdbc:mysql://some-host/somedb?user=someuser&password=somepw" -i

or this

    > gradle build -Dcascading.jdbc.url.postgresql='jdbc:postgresql://some-host/somedb?user=some-user&password=somepw' -i

If you want to build both, you can of course combine the properties.

Debian based systems enable SSL by default for postgres, but are using self signed certificates. If you want to connect
to such a database, you have to add `sslfactory=org.postgresql.ssl.NonValidatingFactory` to the JDBC url, otherwise you
will run into an exception.

## Amazon Redshift

Like the `mysql` and `postgresql` projects the `redshift` project requires a running external database for tests and is
therefore disabled by default. To enable it, provide JDBC URL for a running [Redshift](http://aws.amazon.com/redshift/)
instance to the build as follows:

    > gradle build -Dcascading.jdbc.url.redshift='jdbc:postgresql://some-host/somedb?user=some-user&password=somepw' -i

The Redshift data engine is API-compatible with PostgreSQL and uses the PostgreSQL JDBC driver. Hence the JDBC URL for a
Redshift connection begins with `jdbc:postgresql://`


## Oracle

The `cascading-jdbc-oracle` project contains everything to create jar files to talk to oracle, but due to the fact, that
the oracle jdbc driver is not available on any public maven repository, you have to build it yourself. Here is how:

1. Download the `ojdbc6.jar` file from [oracle technet](http://www.oracle.com/technetwork/database/features/jdbc/index-091264.html)
2. Install it in your local maven repo

    > mvn install:install-file -DgroupId=com.oracle -DartifactId=ojdbc6
           -Dversion=11.2.0.4 -Dpackaging=jar -Dfile=/path/to/ojdbc6.jar -DgeneratePom=true

3. Build the project against an existing oracle database. The user has to be
   able to create and delete tables, in order for the tests to work.

    > gradle cascading-jdbc-oracle:build -Dcascading.jdbc.url.oracle='jdbc:oracle:thin:user/password@host:port:SID' -i

Alternatively to 1. and 2. you can add your organizations maven repository to the build, in case, it already contains
the oracle jdbc driver.

After executing the build you will find all the jars in `cascading-jdbc-oracle/build/libs/`.  You can install them in
your local maven repository with by using `gradle install` instead of `gradle build` or upload them to your
organizations repo manager.


## Teradata

Similar to Oracle above, the `cascading-jdbc-teradata` subproject contains everything to create jar files that can talk
to a teradata instance. Since the teradata JDBC driver is not available in any maven repository, you have to build it
yourself like so:

0. Create a teradata account, if you don't have one already
1. Download `TeraJDBC__indep_indep.14.10.00.39.tar` from the [download page](https://downloads.teradata.com/download/connectivity/jdbc-driver)
2. Unpack the driver:

    > tar xf TeraJDBC__indep_indep.14.10.00.39.tar

3. Install `terajdbc4.jar` and `tdgssconfig.jar` in your local maven repository. You can skip this step, if the teradata
   jars are already in your organizations maven repository.

   > mvn install:install-file -DgroupId=com.teradata -DartifactId=terajdbc4 -Dversion=14.10.00.39 -Dpackaging=jar -Dfile=terajdbc4.jar -DgeneratePom=true
   > mvn install:install-file -DgroupId=com.teradata -DartifactId=tdgssconfig -Dversion=14.10.00.39 -Dpackaging=jar -Dfile=tdgssconfig.jar -DgeneratePom=true


4. Build the sub-project like this:

   > gradle clean cascading-jdbc-teradata:build -Dcascading.jdbc.url.teradata="jdbc:teradata://host/USER=user,PASSWORD=password"

After executing the build you will find all the jars in `cascading-jdbc-teradata/build/libs/`.  You can install them in
your local maven repository with by using `gradle install` instead of `gradle build` or upload them to your
organizations repo manager.

# Usage

## In Cascading applications

With the `JDBCTap` and `JDBCScheme` you can read from database tables, create new tables and write to them, write into
existing tables and, update existing tables. All modes of operation are used in the test base class
`cascading.provider.jdbc.JDBCTestingBase` and should be self explanatory.

Please note that updating a database table is supported, but not recommended for long running jobs. It is considered a
convenience during testing/development.

Typical work loads write into a new table, which is afterwards made available to other parts of your system via a post
process. You can achieve this by using the `onCompleted(Flow flow)` method in a class implementing the
[`cascading.flow.FlowListener`](http://docs.cascading.org/cascading/3.0/javadoc/cascading/flow/FlowListener.html)
interface. In case something goes wrong during the execution of your Flow, you can clean up your database table in the
`onThrowable(Flow flow)` method of your `FlowListener` implementation.

## Maven repository

All artifacts, except the ones for Oracle (see above) are in
[conjars](http://conjars.org).


### Types

Cascading-jdbc supports multiple RDBMS systems all of which behave slightly different when it comes to automatic type
coercions/casts. This project tries to ease the type handling, by doing 'educated guesses', yet it is not always capable
of doing so. We highly recommend that you provide a typed Fields instance to the JDBCScheme constructor, so that values
can be properly coerced before being written to the database.

## In Lingual

__NOTE__: The JDBC providers can only be used on the `hadoop` and `hadoop2-mr1` platforms. The `local` platform is not
supported.

This assumes, that you have followed the [lingual tutorial](http://docs.cascading.org/lingual/2.0/), esp. the part,
where a provider is used to write directly into a memcached server. To accomplish the same, but with a
[derby](http://db.apache.org/derby/) database, you can do the following:

Setup your lingual catalog with the derby provider:

    # only hadoop platform is supported
    > export LINGUAL_PLATFORM=hadoop

    > lingual catalog --provider --add cascading:cascading-jdbc-derby:3.0.0:provider

This will register the provider `derby` for the `hadoop` platform. The provider supports one protocol (`jdbc`) and one
format (`derby`). The provider is downloaded from [conjars](http://conjars.org).

Next we can add the `working` schema, the `titles` stereotype and register the `derby` provider in the schema.

    > lingual catalog --schema working --add
    > lingual catalog --schema working --stereotype titles --add --columns TITLE,CNT --types string,int
    > lingual catalog --schema working --format derby --add --provider derby

Next we set the protocol properties for `jdbc` in the `derby` provider. The first line describes the table, that we are
operating on, to the underlying JDBCTap.  The table has two columns `title` and `cnt`, which are of type `varchar(100)`
and `int`. The command line interface uses `:` as a separator for properties, which contain multiple values.

    > lingual catalog --schema working --protocol jdbc --add --properties="tabledesc.tablename=title_counts,tabledesc.columnnames=title:cnt,tabledesc.columndefs=varchar(100) not null:int not null" --provider derby

__Tip__: If the `--properties` syntax becomes to complex for your use case, you can put them in a properties file and
use `--properties-file /path/to/my.properties` instead.

Finally we tell the derby provider, where it can find the derby server. The `create=true` is optional. If the database
already exists, you can omit it.

    > lingual catalog --schema working --table title_counts --stereotype titles --add "jdbc:derby://localhost:1527/mydb;create=true" --protocol jdbc  --format derby

Now the table `title_counts` is ready to be used from within lingual as a sink.  You can now run a query over the
employe data from the lingual tutorial on hdfs directly into your derby server like this:

    > lingual shell
    (lingual shell)  insert into "working"."title_counts" select title, count( title ) as cnt from employees.titles group by title;
    +-----------+
    | ROWCOUNT  |
    +-----------+
    | 7         |
    +-----------+
    1 row selected (9,581 seconds)

When lingual attempts to write into the underlying JDBC table, like in the example above, it will recreate the table by
default. If you want to append to the table instead, you can use overwrite the SinkMode for the `JDBCTap` by setting
`sinkmode=KEEP`, when registering the table.

The provider can not only be used as a sink, but also as a source, meaning you can investigate the data, that was just
written into the derby table directly from the lingual shell:

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


# Extending and Versioning

Adding a new JDBC system is straight forward. Create a new sub-project and add it to `settings.gradle`. Include the JDBC
driver of your RDBMS in the `build.gradle` file of your sub-project and create a `TestCase` subclassing the
`JDBCTestingBase` explained above. All you have to do, is setting the driver class and JDBC URL for your database. For
an example see
[MysqlTest](https://github.com/Cascading/cascading-jdbc/blob/wip-3.0/cascading-jdbc-mysql/src/test/java/cascading/jdbc/MysqlTest.java).

## JDBC driver versions

The JDBC driver version used for a particular provider should match the version for your database. To use a different
version, edit the `build.gradle` file for the particular driver you're using, change the version specified by the
`compile` line, and re-install the provider.

By default [mavencentral](http://search.maven.org/), [conjars](http://conjars.org/),
[pentaho](http://repository.pentaho.org/) and the local maven install are the only repos listed in the build script. If
the particular version of a driver you need is hosted somewhere else, you will need to add this repo to the top level
`build.gradle` file.

## Provider mechanism

If you want to use your driver as a provider within lingual, you have to include a `provider.properties` in the
`src/main/resources/cascading/bind` directory of your project. The name of the provider should match your database, the
protocol should be set to `jdbc` and the format to the type of database you are using.

Below is an example from the `h2` subproject:

    # default name of provider
    cascading.bind.provider.names=h2
    cascading.bind.provider.h2.platforms=hadoop,hadoop2-mr1

    cascading.bind.provider.h2.factory.classname=cascading.jdbc.JDBCFactory

    # define protocols differentiated by properties
    cascading.bind.provider.h2.protocol.names=jdbc
    cascading.bind.provider.h2.protocol.jdbc.schemes=h2
    cascading.bind.provider.h2.protocol.jdbc.jdbcdriver=org.h2.Driver
    cascading.bind.provider.h2.protocol.jdbc.tabledescseparator=:
    cascading.bind.provider.h2.protocol.jdbc.jdbcuser=
    cascading.bind.provider.h2.protocol.jdbc.jdbcpassword=
    cascading.bind.provider.h2.protocol.jdbc.tabledesc.tablename=
    cascading.bind.provider.h2.protocol.jdbc.tabledesc.columnnames=
    cascading.bind.provider.h2.protocol.jdbc.tabledesc.columndefs=
    cascading.bind.provider.h2.protocol.jdbc.tabledesc.primarykeys=
    cascading.bind.provider.h2.protocol.jdbc.sinkmode=

    # define formats differentiated by properties
    cascading.bind.provider.h2.format.names=h2
    cascading.bind.provider.h2.format.h2.protocols=jdbc
    cascading.bind.provider.h2.format.h2.columnfields=
    cascading.bind.provider.h2.format.h2.separator=:
    cascading.bind.provider.h2.format.h2.columnnames=
    cascading.bind.provider.h2.format.h2.orh2=
    cascading.bind.provider.h2.format.h2.conditions=
    cascading.bind.provider.h2.format.h2.limit=
    cascading.bind.provider.h2.format.h2.updateBy=
    cascading.bind.provider.h2.format.h2.tableAlias=
    cascading.bind.provider.h2.format.h2.selectquery=
    cascading.bind.provider.h2.format.h2.countquery=

For more information on the `provider.properties` file, see the [lingual
documentation](http://docs.cascading.org/lingual/2.0/#_creating_a_data_provider).

# License

All code, unless otherwise noted, is licensed under the Apache Software License version 2.
