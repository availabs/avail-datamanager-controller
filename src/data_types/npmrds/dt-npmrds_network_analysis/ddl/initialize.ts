import createNetworkNodesAndEdgesTables from "./createNetworkNodesAndEdgesTables";
import createNetworkPathsTable from "./createNetworkPathsTables";
import createNetworkNodeIncidentEdges from "./createNetworkNodeIncidentEdges";

export default async function initializeForYear(year: number) {
  await createNetworkNodesAndEdgesTables(year);
  await createNetworkPathsTable(year);
  await createNetworkNodeIncidentEdges(year);
}
