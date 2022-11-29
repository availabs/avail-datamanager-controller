import pandas as pd
import sys
from os import listdir
from os.path import isfile, join


def intersection(lst1, lst2):
    return list(set(lst1) & set(lst2))


def process():
    print 'cleaning data...'

    path = 'data/' + sys.argv[1] + '/'
    files = [f for f in listdir(path) if (isfile(join(path, f)) and f.endswith("csv"))]
    for fileNameI, fileName in enumerate(files):
        print str(fileNameI * 100 / len(files)) + '%'
        csv = pd.read_csv(path + fileName)
        csv = csv.fillna('0')

        numeric_columns = [
            # details
            'begin_yearmonth', 'begin_day', 'begin_time', 'episode_id', 'event_id',
            'end_yearmonth', 'end_day', 'end_time', 'state_fips', 'year',
            'cz_fips', 'injuries_direct', 'injuries_indirect', 'deaths_direct', 'deaths_indirect',
            'begin_range', 'end_range',

            # fatalities
            'fat_yearmonth', 'fat_day', 'fat_time', 'fatality_id', 'event_id', 'fatality_age', 'event_yearmonth',

            # locations
            'yearmonth', 'episode_id', 'event_id', 'location_index', 'lat2', 'lon2'
        ]
        numeric_columns = intersection([x.upper() for x in numeric_columns], list(csv.columns))

        floating_columns = [
            # details
            'tor_length', 'tor_width', 'magnitude', 'begin_lat', 'begin_lon', 'end_lat', 'end_lon',

            # locations
            'range', 'latitude', 'longitude'
        ]

        floating_columns = intersection([x.upper() for x in floating_columns], list(csv.columns))

        other = [
            # details
            'begin_date_time', 'end_date_time',

            # fatalities
            'fatality_date'

            # locations
            'coords_geom'
        ]

        other = intersection([x.upper() for x in other], list(csv.columns))

        convert_dict = {}

        for col in numeric_columns:
            convert_dict[col.upper()] = int

        for col in floating_columns:
            convert_dict[col.upper()] = float

        csv.astype(convert_dict)

        csv.replace(to_replace='\t', value=' ')

        if sys.argv[1] == 'details':
            csv['EVENT_NARRATIVE'] = \
                csv['EVENT_NARRATIVE'] \
                    .astype('string') \
                    .str.replace('\n', ' ') \
                    .str.replace('\t', ' ') \
                    .str.replace('|', '')

            csv['EPISODE_NARRATIVE'] = \
                csv['EPISODE_NARRATIVE'] \
                    .astype('string') \
                    .str.replace('\n', ' ') \
                    .str.replace('\t', ' ') \
                    .str.replace('|', '')

        csv.to_csv(path_or_buf=path + fileName, sep='\t', index=False)


if __name__ == '__main__':
    process()
