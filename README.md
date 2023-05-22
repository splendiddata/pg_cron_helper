# pg_cron_helper
Postgres extension pg_cron_helper can be seen as a wrapper around the [pg_cron](https://github.com/citusdata/pg_cron) extension.

Pg_cron_helper needs extensions pg_cron, postgres_fdw and dblink.

## Installation
First make the pg_cron_helper extension available in the Postgres installation directory either using the pgxs infrastructure:<br>
Make sure pg_config is on the PATH and execute '`sudo make install`'<br>
Or:<br>
execute "`select setting from pg_config() where name = 'SHAREDIR';`" in the database to find the *&lt;SHAREDIR&gt;* directory and copy the pg_cron_helper.control and pg_cron_helper--0.1.sql file into the *&lt;SHAREDIR&gt;*/extension directory.

Also make sure that the pg_cron, postgres_fdw and dblink extensions are available in Postgres.

The pg_cron extension needs setting "`shared_preload_libraries = 'pg_cron'`" to be present in the postgresql.conf file (findable via "`select setting from pg_settings where name = 'config_file';`"). Postgres needs to be restarted after adding that setting. See [pg_cron description](https://github.com/citusdata/pg_cron#setting-up-pg_cron).

Then the pg_cron extension must be installed on the cron server database ('postgres' by default -- see the installation instructions of the pg_cron extension).<br>
After installing pg_cron, the pg_cron_helper must be installed in the same cron server database using "`create extension pg_cron_helper cascade;`" ("cascade" makes sure extensions postgres_fdw and dblink are installed as well).

If you want to make pg_cron functionality available in more than one database in the Postgres cluster, then you can install the pg_cron_helper extension in these databases as well. Just execute "`create extension pg_cron_helper cascade;`"

![img](pictures/deployment.png)