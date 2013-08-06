# cascading-provider-jdbc

A set cascading provider which interact with RDBMS systems via JDBC. The project
consists of a generic part `cascading-jdbc-core` and database specific
sub-projects. Each of the subproject will produce a `provider` jar, which is
compatible with the cascading provider mechanism.

__NOTE:__ This is work in progress and can for now change anytime. Please do not
use it in production applications yet.
