from osgeo import gdal
from osgeo import ogr
import os
import schedule
import time

dbServer = "119.59.125.134"
dbName = "data"
dbUser = "postgres"
dbPW = "Pgis@rti2dss@2020"


def interp(col, tiffpath, shppath):
    tbs = ["v_pcd_aqi_d1", "v_pcd_aqi_d2", "v_pcd_aqi_d3",
           "v_pcd_aqi_d4", "v_pcd_aqi_d5", "v_pcd_aqi_d6",
           "v_pcd_aqi_d7", "v_pcd_aqi_d8", "v_pcd_aqi_d9",
           "v_pcd_aqi_d10", "v_pcd_aqi_d11", "v_pcd_aqi_d12",
           "v_pcd_aqi_d13", "v_pcd_aqi_d14", "v_pcd_aqi_av14"]
    for tb in tbs:
        sql = '''SELECT sta_id, {col}, 
            ST_Transform(geom, 32647) as geom FROM {tb}'''.format(
            col=col, tb=tb)
        cmd = '''ogr2ogr -overwrite -f \"ESRI Shapefile\" {shppath}{shp_name}.shp PG:"host={host} user={username} dbname={db} password={password}" -sql "{sql}"'''.format(
            shppath=shppath, shp_name=tb, host=dbServer, username=dbUser, db=dbName, password=dbPW, sql=sql)
        os.system(cmd)

        out = tiffpath+col+"_"+tb+".tif"
        print(out)

        idw = gdal.Grid(out, shppath+tb+".shp", zfield=col,
                        algorithm="invdist")
        idw = None


def runSched():
    colList = ["aqi", "pm25", "pm10", "co", "o3", "so2", "no2"]
    for col in colList:
        tiffpath = "./tiff_"+col+"/"
        shppath = "./shp/"
        interp(col, tiffpath, shppath)

    print("do not close this window!!")


if __name__ == '__main__':
    schedule.every().day.at("00:30").do(runSched)
    while True:
        schedule.run_pending()
        time.sleep(1)
