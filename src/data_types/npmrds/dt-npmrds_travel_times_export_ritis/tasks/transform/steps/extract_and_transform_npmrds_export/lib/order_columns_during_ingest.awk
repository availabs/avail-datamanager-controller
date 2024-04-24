#!/usr/bin/awk

# https://dba.stackexchange.com/a/305551

BEGIN {
  FS=","
    C["tmc_code"] = 1
    C["measurement_tstamp"] = 2
    C["speed"] = 3
    C["historical_average_speed"] = 4
    C["reference_speed"] = 5
    C["travel_time_seconds"] = 6
    C["data_density"] = 7
}
NR == 1 {
  # determine the New Order
  for (N=1;N<=NF;N++) {
    NO[N] = C[$N]
  }
  for (CO in NO) {
    NCO[CO] = NO[CO]
  }
}
# If the travel_time_seconds column is not empty, print the row.
$NCO[NO[6]] != "" {
  OFS=","
  # print $1,$2,$3,$4,$5
  print $NCO[NO[1]],$NCO[NO[2]],$NCO[NO[3]],$NCO[NO[4]],$NCO[NO[5]],$NCO[NO[6]],$NCO[NO[7]]
}
