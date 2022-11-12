export function getTimestamp() {
  const isoString = new Date().toISOString();

  const timestamp = isoString.replace(/[^A-Z0-9]/g, "");

  return timestamp;
}
