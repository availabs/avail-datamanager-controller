[![Moleculer](https://badgen.net/badge/Powered%20by/Moleculer/0e83cd)](https://moleculer.services)

# avail-datamanager-controller

## Configuration

Configure by creating a .env file for the instance:

```sh
$ cat dama_dev_1.env
PORT=3369
DAMA_PG_ENV=dama_dev_1
```

There MUST be a corresponding postgres configuration file:

```sh
$ cat config/postgres.dama_dev_1.env
PGDATABASE=dama_dev_1
PGUSER=dama_dev_user
# $ uuidgen # Can be used to generate PGPASSWORD
PGPASSWORD=9b5eb72e-0b59-49b3-95df-62998ca2714e
PGHOST=127.0.0.1
PGPORT=5466
```

See ./docker/README.md on how to run a development database.

## Gradual Task Integration

```sh
mkdir -p tasks
```

```sh
cd ./tasks/gis-data-integration
git pull
npm install
```

```sh
cd ./tasks/NPMRDS_Database
git pull
npm install
cd config
ln -fs ../../../config/postgres.development.env postgres.env.dev
```

## Starting

```sh
./start dama_dev_1
```

## Moleculer

This [Moleculer](https://moleculer.services/)-based microservices project was
generated with the [Moleculer CLI](https://moleculer.services/docs/0.14/moleculer-cli.html).

After starting, open the http://localhost:3000/ URL in your browser.
On the welcome page you can test the generated services via API Gateway and check the nodes & services.

In the terminal, try the following commands:

-   `nodes` - List all connected nodes.
-   `actions` - List all registered service actions.
-   `call greeter.hello` - Call the `greeter.hello` action.
-   `call greeter.welcome --name John` - Call the `greeter.welcome` action with the `name` parameter.
-   `call products.list` - List the products (call the `products.list` action).

## Services

-   **api**: API Gateway services
-   **greeter**: Sample service with `hello` and `welcome` actions.
-   **products**: Sample DB service. To use with MongoDB, set `MONGO_URI` environment variables and install MongoDB adapter with `npm i moleculer-db-adapter-mongo`.

## Mixins

-   **db.mixin**: Database access mixin for services. Based on [moleculer-db](https://github.com/moleculerjs/moleculer-db#readme)

## Useful links

-   Moleculer website: https://moleculer.services/
-   Moleculer Documentation: https://moleculer.services/docs/0.14/

## NPM scripts

-   `npm run dev`: Start development mode (load all services locally with hot-reload & REPL)
-   `npm run start`: Start production mode (set `SERVICES` env variable to load certain services)
-   `npm run cli`: Start a CLI and connect to production. Don't forget to set production namespace with `--ns` argument in script
-   `npm run lint`: Run ESLint
-   `npm run ci`: Run continuous test mode with watching
-   `npm test`: Run tests & generate coverage report
-   `npm run dc:up`: Start the stack with Docker Compose
-   `npm run dc:down`: Stop the stack with Docker Compose
