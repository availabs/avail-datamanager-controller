# NpmrdsAuthoritativeTravelTimesDb RULES

Abbreviations:

-   NpmrdsAuthoritativeTravelTimesDb (ATT)
-   NpmrdsTravelTimesExportDb (ETT)

```
  1. Cannot create data gaps:

    a. IF previous month exists in the ATTs
        THEN cannot add a new month unless the range between
        the immedidate precessor month to the new month is complete.

    b. IF a later month exists in the ATTs, symmetric to 1.a.

    c. IF the ATTs for a StateYearMonth currently span a date range,
        the new authoritative ATTs must cover at least that range.

  2. New NpmrdsAuthoritativeTravelTimesDb DamaView must have at least the same
     date range per state as the old NpmrdsAuthoritativeTravelTimesDb DamaView.

  3. ROLLBACKs: Must support rolling back changes.

    a. Can ROLLBACK to a previous DamaView's state by cloning that view's data
        as the current ATT DamaView.

        However, but cannot do ETT level undos as that would necessarily
          violate a subrule from 1.

```
