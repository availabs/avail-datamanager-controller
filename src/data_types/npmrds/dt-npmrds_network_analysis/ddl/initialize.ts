import createNetworkNodesAndEdgesTables from "./createNetworkNodesAndEdgesTables";
import createNetworkPathsTable from "./createNetworkPathsTables";
import createNetworkNodeIncidentEdges from "./createNetworkNodeIncidentEdges";
import createNetworkNodeDescriptions from "./createNetworkNodeDescriptions";
import createTmcDescriptions from "./createTmcDescriptions";
import createOverlapsTable from "./createOverlapsTable";
import createNpmrdsNetworkTmcDynamicReferenceView from "./createNpmrdsNetworkTmcDynamicReferenceView";

export default async function initializeForYear(year: number) {
  await createNetworkNodesAndEdgesTables(year);
  await createNetworkPathsTable(year);
  await createNetworkNodeIncidentEdges(year);
  await createNetworkNodeDescriptions(year);
  await createTmcDescriptions(year);
  await createOverlapsTable(year);
  await createNpmrdsNetworkTmcDynamicReferenceView(year);
}
