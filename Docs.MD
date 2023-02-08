
1 - Dama Configurations
 - Database
   Database config files live directly in config. The name of the file has to be postgres.${Instance}.env, instance name has to match with falcor.

 All Other config is ENVIRONMENT variables, that are in .env

2 - data-manager-controler Code structure
  /config - database configuration files
  /dama-mbtiles - processed mbtiles from uploads
  /data - processed data from uploads. Contents are served via express at /files/
  /docker - code to run local dev database in docker
  /temp-etl - directory for temporary data for processes


4 - data-manager database structure

3 - Standard Conventions for processes
