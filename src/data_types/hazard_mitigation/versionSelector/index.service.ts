import { Context } from "moleculer";
import load from "./loadVersions";
import serviceName from "../constants/serviceName";

export default {
  name: `${serviceName}.versionSelectorUtils`,
  actions: {
    load,
  },
};
