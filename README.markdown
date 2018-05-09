gaetk_replication
=================

Automates replication from AppEngine to CloudSQL

For BigQuery functionality use
http://appengine-toolkit2.readthedocs.io/en/latest/backupreplication.html


Usage: CloudSQL
---------------

* create a CloudSQL instance in the [Google API Console][1], allow your Application to access it. See [Getting Started][2] for more Details. We assume it is named `mytestinstance`
* create a Database inside your instance: `CREATE DATABASE test;`

Now you can add gaetk_replication to your project:

    mkdir -p lib
    git submodule add git@github.com:hudora/gaetk_replication.git lib/gaetk_replication
    echo "gaetk_replication_SQL_INSTANCE_NAME = 'something:mytestinstance'" >> appengine_config.py
	echo "gaetk_replication_SQL_DATABASE_NAME = 'test'" >> appengine_config.py

Adjust names to match your setup.

Now add the replicatior to your `app.yaml`:

    handlers:
    - url: /gaetk_replication/.*
      script: replication.cloudsql.application
      login: admin

That's it. Deploy and call `/gaetk_replication/cloudsql/cron`.


[1]: https://code.google.com/apis/console
[2]: https://developers.google.com/cloud-sql/docs/before_you_begin


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/hudora/gaetk_replication/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

