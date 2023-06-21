import createTmcShapeSimilarityTable from "./createTmcSimilarityTables";
import matchingFirstPass from "./matchingFirstPass";
import matchingSecondPass from "./matchingSecondPass";

export default async function main(year_a: number, year_b: number) {
  await createTmcShapeSimilarityTable(year_a, year_b);
  await matchingFirstPass(year_a, year_b);
  await matchingSecondPass(year_a, year_b);
}
