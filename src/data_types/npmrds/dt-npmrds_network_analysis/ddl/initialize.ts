import createNetworkNodesAndEdgesTables from "./createNetworkNodesAndEdgesTables";
import createNetworkPathsTable from "./createNetworkPathsTables";
import createNetworkNodeIncidentEdges from "./createNetworkNodeIncidentEdges";
import createNetworkNodeLevel1Labels from "./createNetworkNodeLabels";

export default async function initializeForYear(year: number) {
  await createNetworkNodesAndEdgesTables(year);
  await createNetworkPathsTable(year);
  await createNetworkNodeIncidentEdges(year);
  await createNetworkNodeLevel1Labels(year);
}
