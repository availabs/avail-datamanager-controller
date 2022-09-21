import { join } from "path";

import libDir from "../../../../constants/libDir";

export const tippecanoeDir = join(libDir, "tippecanoe");

// Path to the tippecanoe executable
export default join(tippecanoeDir, "tippecanoe");
