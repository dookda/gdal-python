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
    print(datetime.date.today())


if __name__ == '__main__':
    # runSchedTMD_3hour()
    schedule.every().minute.at(":53").do(runSchedTMD_3hour)
    while True:
        schedule.run_pending()
        time.sleep(1)
