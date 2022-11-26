export function getTimestamp(date: Date = new Date()) {
  const isoString = date.toISOString();

  const timestamp = isoString.replace(/[^A-Z0-9]/g, "");

  return timestamp;
}
