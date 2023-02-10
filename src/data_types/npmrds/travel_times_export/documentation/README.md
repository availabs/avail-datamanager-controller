# NPMRDS Travel Times Export Downloader

## Design

### Subprocess with idle timeout

The actual NpmrdsDownloadService runs in a subprocess.
While running the subprocess contains a headless Puppeter instance
with two open pages.
The subprocess is killed after it is idle for 1 hour.
This serves two purposes.

The first is that is does not waste resources locally or at the data source.

The second is that the NpmrdsTravelTime data available date extent
is statically written into one of the two web pages.
The idle timeout is currently how the date extent is refreshed.
Implementing a request/response refresh will be more complicated.
