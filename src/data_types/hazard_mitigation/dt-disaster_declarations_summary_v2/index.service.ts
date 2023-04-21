import { Context } from "moleculer";
import load from "./loadData";
import serviceName from "../constants/serviceName";

export default {
  name: `${serviceName}.disaster_declarations_summary_v2`,
  actions: {
    load,
  },
};
