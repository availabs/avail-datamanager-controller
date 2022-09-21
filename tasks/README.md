# legacy_etl_code

Like eventual consistency, eventual integration.

NOTE:

-   Making Tasks depend on DaMaAdmin modules removes the ability to run them in isolation.
-   There should be a higher level that handles the configuration of Tasks and Services.
-   Concerning code, reusable code should be moved to the DaMa modules. If they are
    required elsewhere, maybe the belong in npm modules.
