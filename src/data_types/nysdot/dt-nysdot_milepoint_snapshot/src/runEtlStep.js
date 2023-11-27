const { join } = require('path')

const execa = require('execa')

const base_psql_config = ['-c', '\\timing on']
// const base_psql_config = []

async function runEtlStep(step_name, config) {
  config = {
    in_transaction : true,
    psql_env : {},
    psql_args : [],
    ...(config || etl_step_configs[step_name])
  }

  const {
    sql_dir,
    in_transaction,
    rel_path,
    rel_paths,
    psql_env,
    psql_args
  } = config

  console.log()
  console.log('-'.repeat(30))
  console.log('START:', step_name)
  console.group()
  console.time(step_name)

  console.log()

  const sql_paths = [];

  if (rel_path) {
    sql_paths.push(join(sql_dir, rel_path))
  }

  if (Array.isArray(rel_paths)) {
    sql_paths.push(...rel_paths.map(p => join(sql_dir, p)))
  }

  const args = base_psql_config.slice()

  if (in_transaction) {
    args.push('--single-transaction')
  }

  for (const k of Object.keys(psql_env)) {
    const v = psql_env[k]

    args.push('-v', `${k}=${v}`)
  }

  const {
    SOURCE_DATA_SCHEMA,
    ETL_WORK_SCHEMA
  } = process.env

  for (const sql_path of sql_paths) {
    args.push(
      '-f', sql_path,
      '-v', 'ON_ERROR_STOP=1',
      '-v', `SOURCE_DATA_SCHEMA=${SOURCE_DATA_SCHEMA}`,
      '-v', `ETL_WORK_SCHEMA=${ETL_WORK_SCHEMA}`,
      ...psql_args
    )

    await execa('psql', args, { stdio: 'inherit' })
  }

  console.log()
  console.timeEnd(step_name)
  console.groupEnd()

  console.log()
  console.log('DONE:', step_name)
  console.log()
  console.log('-'.repeat(30))
  console.log()
}

module.exports = runEtlStep
