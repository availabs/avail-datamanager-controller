# Docker Development Database

## CONFIGURATION

### Configuring the development database

See: [dockerhub postgis/postgis](https://registry.hub.docker.com/r/postgis/postgis)

Note: the Postgis Docker image environment variable names differ from the corresponding
[PostgreSQL environment variables](https://www.postgresql.org/docs/11/libpq-envars.html).

```sh
$ cat ../config/postgres.docker.env
# Docker specific
#   Having the postgres.env variables in this file causes containser startup to fail.
#   see https://hub.docker.com/_/postgres/
POSTGRES_DB=dama_dev_db
POSTGRES_USER=dama_dev_user
POSTGRES_PASSWORD=__CHANGE_THIS__
```

```sh
$ cat docker-compose.yml
version: '2'
services:
db:
image: postgis/postgis:11-3.3
container_name: dama_dev_db
ports: - "5465:5432"
volumes: - ./pg_data:/data - ./sqlScripts/:/sqlScripts/
env_file: "../config/postgres.docker.env"
environment: - PGDATA=/pg_data
```

### Configuring the app to connect to the development database

```sh
# https://moleculer.services/docs/0.12/runner.html#env-files
$ cat ../.env
DAMA_PG_ENV=development
```

```sh
$ cat ../config/postgres.development.env
PGDATABASE=dama_dev_db
PGUSER=dama_dev_user
# echo $RANDOM | md5sum
PGPASSWORD=__CHANGE_THIS__
PGHOST=127.0.0.1
PGPORT=5465
```

See the [PostgreSQL environment variables](https://www.postgresql.org/docs/11/libpq-envars.html)
documentation for further options.

## USAGE

Note: You may need to use `sudo` to execute the scripts below.

Start the development database.

```sh
./up
```

Stop the development database

```sh
./down
```
