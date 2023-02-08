
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
  /lib - For static version of external libraries
  /notes - developer notes
  /public - static files served
  /src 
  	/constansts/ - reads configs and makes them available
  	/data_manager/
	  	/dama_api_server/ - registers all routes for server
	  	/dama_db/ - reads db configs and makes connections available
	  	/dama_dispatcher/ - functions for interacting with event_store and etl_context
	  	/dama_gis/ - To be moved
	  	/dama_integration/ - to be moved
	  	/dama_meta/ service to get db info schema data
	  	/dama_tileserver/ - code and config for tileserver
	  	/dama_utils/ - universal utility functions

	/data_types/ 
	  	-A list of all data types that are available in the server
	  	- data types may be organized into folders
	  	- any data type should export its own moleculer service
	  	- data_type folders should have name convention dt-{type_name}
	  	- data_type folders should have index.service.(js/ts)
	  	- every data type must have its own directory and service.
	  	- data types should denote if they are single source, or multi source by putting that into source metadata in the create step.

	/data_utils/





  /tasks - to be gotten rid of

4 - data-manager database structure

3 - Standard Conventions for processes


Data Types
 - Data Types are code for processing data into the dama server. They can load external data or process existing data
 
 - A datatype should have
   - a create step with creates initial source at least
   - should create source metadata if applicable
   - ability to load new view for existing source
   





