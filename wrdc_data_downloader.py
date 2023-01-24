import os
import urllib.request
import pandas as pd



def wrdc_data_download_hour(station, years, rad_data, out_folder):
    '''

    :param station: station name from wrdc data; see file station_names.txt
    :param years: years to download from start to end
    :param rad_data: what radiation data (glo - global, dif - diffuse,
    dir - direct, uwl - uplongwave, dwl - downlongwave)
    :param out_folder: folder to save csv file
    :return: None
    '''

    df_all = pd.DataFrame(columns=rad_data)

    years = range(years[0], years[1]+1)

    out_folder_station = out_folder + station

    try:
        os.mkdir(out_folder_station)
    except:
        pass

    for year in years:

        year = str(year)
        out_folder_station_year = out_folder_station + '/' + year

        try:
            os.mkdir(out_folder_station_year)
        except:
            pass

        df_rad = pd.DataFrame(columns=rad_data)

        for rad in rad_data:
            # hourly wrdc data url creation
            url_wrdc = 'http://wrdc.mgo.rssi.ru/wrdccgi/protect.exe?GAW_DATA/' + year + '/' + station + '_' + year + '_' + rad + '_h.htm'

            out_folder_station_year_rad = out_folder_station_year + '/' + rad + '.htm'
            urllib.request.urlretrieve(url_wrdc, out_folder_station_year_rad)

            df = pd.read_html(out_folder_station_year_rad)[0]
            df.index = df.iloc[:, 0].astype('str')
            df_columns = df.iloc[1]

            rows_to_drop = ['Total',
                            'MEAN',
                            'Year ' + year + ' January',
                            'Year ' + year + ' February',
                            'Year ' + year + ' March',
                            'Year ' + year + ' April',
                            'Year ' + year + ' May',
                            'Year ' + year + ' June',
                            'Year ' + year + ' July',
                            'Year ' + year + ' August',
                            'Year ' + year + ' September',
                            'Year ' + year + ' October',
                            'Year ' + year + ' November',
                            'Year ' + year + ' December']

            columns_to_drop = ['TOTAL', 'F']

            try:
                df.drop(rows_to_drop, axis=0, inplace=True)
            except:
                continue

            df = df.set_axis(df_columns, axis=1)
            df.drop(columns_to_drop, axis=1, inplace=True)
            df.drop('TIME DATE', axis=0, inplace=True)
            df.drop('TIME DATE', axis=1, inplace=True)
            doy = range(1, len(df) + 1)
            df['doy'] = doy
            # df['doy_time'] = df['doy'].astype(str) + df['TIME DATE']
            df_melt = df.melt(id_vars='doy')
            df_melt = df_melt.sort_values(by=['doy', 'TIME DATE'])
            df_melt['year'] = str(year)
            df_melt['doy'] = (df_melt['doy']
                              .astype('str')
                              .apply(lambda x: '0' + x if len(x) == 2 else x)
                              .apply(lambda x: '00' + x if len(x) == 1 else x)
                              )

            dt = df_melt['year'] + df_melt['doy'].astype('str') + df_melt['TIME DATE'].apply(lambda x: x[0:2])
            df_melt['Datetime'] = pd.to_datetime(dt, format="%Y%j%H")
            df_melt.index = df_melt['Datetime']
            df_melt = df_melt['value']
            df_rad[rad] = df_melt

        df_all = pd.concat([df_all, df_rad], join='outer')

    csv_out = out_folder_station+'.csv'
    df_all.to_csv(csv_out)
