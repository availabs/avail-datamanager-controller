const pgEnv = process.env.DAMA_PG_ENV || "development";

console.log("%%%%% PostgreSQL Environment:", pgEnv, "%%%%%");

export default pgEnv;
