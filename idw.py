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
        sql = "SELECT ST_SetSRID(ST_Makepoint(lon, lat), 4326) as geom, sta_num, avg(max_temp) as avg_temp, avg(rh) as avg_rh, sum(rainfall) as sum_rain FROM weather_daily_tmd WHERE  extract(week from datetime) = {wk} and   (province = 'ระยอง' OR   province = 'ชลบุรี' OR   province = 'ฉะเชิงเทรา' OR   sta_num = '48420' OR   sta_num = '48429' OR  sta_num = '48430' OR  sta_num = '48439' OR  sta_num = '48440' OR  sta_num = '48480' OR  sta_num = '48481') GROUP BY extract(week from datetime), sta_num, sta_th, geom".format(
            wk=wk)

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


def anualInter():
    tiffpath = "./tiff_tmd/"
    shppath = "./shp_tmd/"
    sql = '''SELECT ST_SetSRID(ST_Makepoint(lon, lat), 4326) as geom, sta_num, avg(max_temp) as avg_temp, avg(rh) as avg_rh, sum(rainfall) as sum_rain FROM weather_daily_tmd WHERE (province = 'ระยอง' OR province = 'ชลบุรี' OR province = 'ฉะเชิงเทรา' OR sta_num = '48420' OR sta_num = '48429' OR sta_num = '48430' OR sta_num = '48439' OR sta_num = '48440' OR sta_num = '48480' OR sta_num = '48481') GROUP BY sta_num, sta_th, lon, lat'''

    tmd_shp = '''{shppath}tmd_anual.shp'''.format(shppath=shppath)

    cmd = '''ogr2ogr -overwrite -f \"ESRI Shapefile\" {out_shp} PG:"host={host} user={username} dbname={db} password={password}" -sql "{sql}"'''.format(
        out_shp=tmd_shp, host=dbServer, username=dbUser, db=dbName, password=dbPW, sql=sql)
    os.system(cmd)

    out_rain = '''{tiffpath}rain_anual.tif'''.format(
        tiffpath=tiffpath)
    out_rh = '''{tiffpath}rh_anual.tif'''.format(
        tiffpath=tiffpath)
    out_temp = '''{tiffpath}temp_anual.tif'''.format(
        tiffpath=tiffpath)
    print(out_rain)

    gdal.Grid(out_rain, tmd_shp, zfield="sum_rain",
              algorithm="invdist:power=3")
    gdal.Grid(out_rh, tmd_shp, zfield="avg_rh",
              algorithm="invdist:power=3")
    gdal.Grid(out_temp, tmd_shp, zfield="avg_temp",
              algorithm="invdist:power=3")


def aqiInterp(col, tiffpath, shppath):
    tbs = ["v_pcd_aqi_d1", "v_pcd_aqi_d2", "v_pcd_aqi_d3",
           "v_pcd_aqi_d4", "v_pcd_aqi_d5", "v_pcd_aqi_d6",
           "v_pcd_aqi_d7", "v_pcd_aqi_d8", "v_pcd_aqi_d9",
           "v_pcd_aqi_d10", "v_pcd_aqi_d11", "v_pcd_aqi_d12",
           "v_pcd_aqi_d13", "v_pcd_aqi_d14", "v_pcd_aqi_av14"]
    for tb in tbs:
        sql = "SELECT sta_id, {col}, ST_Transform(geom, 32647) as geom FROM {tb}".format(
            col=col, tb=tb)
        cmd = '''ogr2ogr -overwrite -f \"ESRI Shapefile\" {shppath}{shp_name}.shp PG:"host={host} user={username} dbname={db} password={password}" -sql "{sql}"'''.format(
            shppath=shppath, shp_name=tb, host=dbServer, username=dbUser, db=dbName, password=dbPW, sql=sql)
        os.system(cmd)

        out = tiffpath+col+"_"+tb+".tif"
        print(out)

        idw = gdal.Grid(out, shppath+tb+".shp", zfield=col,
                        algorithm="invdist:power=3")
        idw = None


def tempInter():
    tiffpath = "./tiff_temp3hour/"
    shppath = "./shp_tmd/"
    sql = '''SELECT * FROM v_tmd_weather_3hr_eec'''

    tmd_shp = '''{shppath}temp3hour.shp'''.format(shppath=shppath)

    cmd = '''ogr2ogr -overwrite -f \"ESRI Shapefile\" {out_shp} PG:"host={host} user={username} dbname={db} password={password}" -sql "{sql}"'''.format(
        out_shp=tmd_shp, host=dbServer, username=dbUser, db=dbName, password=dbPW, sql=sql)
    os.system(cmd)

    out_temp = '''{tiffpath}temp_3hour.tif'''.format(
        tiffpath=tiffpath)

    gdal.Grid(out_temp, tmd_shp, zfield="air_temp",
              algorithm="invdist:power=3")
    print("ok")


def runSchedTMD_3hour():
    tempInter()


def runSchedAQI():
    colList = ["aqi", "pm25", "pm10", "co", "o3", "so2", "no2"]
    for col in colList:
        tiffpath = "./tiff_"+col+"/"
        shppath = "./shp/"
        aqiInterp(col, tiffpath, shppath)

    print("do not close this window!!")


def runSchedTMD():
    # rain interpolation
    wks = []
    # wks = [2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16,
    #        17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29]

    _date = datetime.date.today()
    year, week_num, day_of_week = _date.isocalendar()
    print("Week #" + str(week_num))
    wks.append(week_num)
    rainInterp(wks)


def runSchedTMD_anual():
    anualInter()
    print(datetime.date.today())


if __name__ == '__main__':
    schedule.every().hour.do(runSchedTMD_3hour)
    schedule.every().day.at("07:31").do(runSchedTMD_anual)
    schedule.every().day.at("07:32").do(runSchedTMD)
    schedule.every().day.at("05:34").do(runSchedAQI)
    while True:
        schedule.run_pending()
        time.sleep(1)
