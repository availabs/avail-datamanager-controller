import createNetworkNodesAndEdgesTables from "./createNetworkNodesAndEdgesTables";
import createNetworkPathsTable from "./createNetworkPathsTables";
import createNetworkNodeIncidentEdges from "./createNetworkNodeIncidentEdges";
import createOverlapsTable from "./createOverlapsTable";

export default async function initializeForYear(year: number) {
  // await createNetworkNodesAndEdgesTables(year);
  // await createNetworkPathsTable(year);
  // await createNetworkNodeIncidentEdges(year);
  await createOverlapsTable(year);
}
