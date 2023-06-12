export function getTimestamp(date: Date = new Date()) {
  const isoString = date.toISOString();

  const timestamp = isoString
    .replace(/[^A-Z0-9]/g, "")
    .toLowerCase()
    .slice(0, -4);

  return timestamp;
}

export const sleep = (time_ms: number) =>
  new Promise((r) => setTimeout(r, time_ms));
