import createCrossYearDynamicReferenceView from "./createCrossYearDynamicReferenceView";

export default async function main(year_a: number, year_b: number) {
  await createCrossYearDynamicReferenceView(year_a, year_b);
}
