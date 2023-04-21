import { Context } from "moleculer";
import load from "./loadData";
import serviceName from "../constants/serviceName";

export default {
  name: `${serviceName}.ihp_v1`,
  actions: {
    load,
  },
};
