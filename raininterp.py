from osgeo import gdal
from osgeo import ogr
import os
import schedule
import time
import datetime

dbServer = "119.59.125.134"
dbName = "data"
dbUser = "postgres"
dbPW = "Pgis@rti2dss@2020"


def rainInterp(wks):
    tiffpath = "./tiff_tmd/"
    shppath = "./shp_tmd/"
    for wk in wks:
        print(wk)
        sql = '''SELECT ST_Transform(ST_SetSRID(ST_Makepoint(lon, lat), 4326), 32647) as geom, sta_num, avg(max_temp) as avg_temp, avg(rh) as avg_rh, sum(rainfall) as sum_rain
        FROM weather_daily_tmd
        WHERE  extract(week from datetime) = {wk} and 
            (province = 'ระยอง' OR 
            province = 'ชลบุรี' OR 
            province = 'ฉะเชิงเทรา' OR 
            sta_num = '48420' OR 
            sta_num = '48429' OR 
            sta_num = '48430' OR 
            sta_num = '48439' OR 
            sta_num = '48440' OR 
            sta_num = '48480' OR 
            sta_num = '48481')
        GROUP BY extract(week from datetime), sta_num, sta_th, lon, lat'''.format(wk=wk)

        tmd_shp = '''{shppath}tmd_w{wk}.shp'''.format(
            shppath=shppath, wk=wk)

        cmd = '''ogr2ogr -overwrite -f \"ESRI Shapefile\" {out_shp} PG:"host={host} user={username} dbname={db} password={password}" -sql "{sql}"'''.format(
            out_shp=tmd_shp, host=dbServer, username=dbUser, db=dbName, password=dbPW, sql=sql)
        os.system(cmd)

        out_rain = '''{tiffpath}rain_w{wk}.tif'''.format(
            tiffpath=tiffpath, wk=wk)
        out_rh = '''{tiffpath}rh_w{wk}.tif'''.format(
            tiffpath=tiffpath, wk=wk)
        out_temp = '''{tiffpath}temp_w{wk}.tif'''.format(
            tiffpath=tiffpath, wk=wk)
        print(out_rain)

        gdal.Grid(out_rain, tmd_shp, zfield="sum_rain",
                  algorithm="invdist:power=3")
        gdal.Grid(out_rh, tmd_shp, zfield="avg_rh",
                  algorithm="invdist:power=3")
        gdal.Grid(out_temp, tmd_shp, zfield="avg_temp",
                  algorithm="invdist:power=3")


if __name__ == '__main__':
    wks = []
    wks = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
           17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]
    _date = datetime.date.today()
    year, week_num, day_of_week = _date.isocalendar()
    print("Week #" + str(week_num))
    # wks.append(week_num)
    rainInterp(wks)
    # schedule.every().day.at("09:00").do()
    # while True:
    #     schedule.run_pending()
    #     time.sleep(1)
