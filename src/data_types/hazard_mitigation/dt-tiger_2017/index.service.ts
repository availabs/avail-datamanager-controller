import { Context } from "moleculer";
import load from "./loadData";
import serviceName from "../constants/serviceName";

export default {
  name: `${serviceName}.tigerDownloadAction`,
  actions: {
    load,
  },
};
