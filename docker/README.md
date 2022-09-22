# Docker Development Database

## CONFIGURATION

### Configuring the development database

Note: the Postgis Docker image environment variable names differ from the corresponding
[PostgreSQL environment variables](https://www.postgresql.org/docs/11/libpq-envars.html).

```sh
$ cat ../config/postgres.docker.env
# Docker specific
#   Having the postgres.env variables in this file causes containser startup to fail.
#   see https://hub.docker.com/_/postgres/
POSTGRES_DB=dama_dev_db
POSTGRES_USER=dama_dev_user
POSTGRES_PASSWORD=# $ uuidgen # Use this to generate UUIDs
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

See [dockerhub postgis/postgis](https://registry.hub.docker.com/r/postgis/postgis)
for further details.

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

PGPASSWORD=# $ uuidgen # Use this to generate UUIDs
PGHOST=127.0.0.1
PGPORT=5465
```

See [PostgreSQL environment variables](https://www.postgresql.org/docs/11/libpq-envars.html)
for further details.

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
