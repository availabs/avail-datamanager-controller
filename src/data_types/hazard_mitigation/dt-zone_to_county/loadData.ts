import { Context } from "moleculer";
import csvUploadAction from "../csvUploadAction"
export default async function publish(ctx: Context) {
  return csvUploadAction(ctx)
}
