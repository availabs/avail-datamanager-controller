const pgEnv = process.env.DAMA_PG_ENV || "development";

if (pgEnv !== "development") {
  throw new Error("Whoa!!! This project is not ready for prime time.");
}

console.log("%%%%% PostgreSQL Environment:", pgEnv, "%%%%%");

export default pgEnv;
