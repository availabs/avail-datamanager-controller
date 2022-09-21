export default (dataSourceName: string) =>
  dataSourceName.split(/\//).slice(-2).join("/");
