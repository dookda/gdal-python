from osgeo import gdal
from osgeo import ogr
import os

# connect shp
# pts = ogr.Open("vill_32647.shp", 0)
# layer = pts.GetLayer()

# print(layer)

# for fld in layer.schema:
#     print(fld.name)

# connect db
# dbServer = "119.59.125.134"
# dbName = "data"
# dbUser = "postgres"
# dbPW = "Pgis@rti2dss@2020"

# connString = "PG: host=%s dbname=%s user=%s password=%s" % (
#     dbServer, dbName, dbUser, dbPW)
# conn = ogr.Open(connString)

# for i in conn:
#     daLayer = i.GetName()
#     print(i)

# lyr = conn.GetLayer('v_pcd_aqi_d1')
# print(lyr)

# for fld in lyr.schema:
#     print(fld.name)

# cal extent
# ras = gdal.Open("ras.tif")
# gt = ras.GetGeoTransform()

# ulx = gt[0]
# uly = gt[3]
# res = gt[1]

# xsize = ras.RasterXSize
# ysize = ras.RasterYSize

# lrx = ulx + xsize * res
# lry = ulx - ysize * res

# print(ulx)
# print(uly)
# print(lrx)
# print(lry)

# ras = None
# pts = layer = None

# interpolation
# nn = gdal.Grid("nn.tif", "vill_32647.shp", zfield="rand",
#                algorithm="nearest",
#                outputBounds=[ulx, uly, lrx, lry],
#                width=xsize, height=ysize)


# ogr2ogr - f "ESRI Shapefile" mydata.shp PG: "host=myhost user=myloginname dbname=mydbname password=mypassword" - sql "SELECT name, the_geom FROM neighborhoods"

dbServer = "119.59.125.134"
dbName = "data"
dbUser = "postgres"
dbPW = "Pgis@rti2dss@2020"
shp_out = "v_pcd_aqi_d1"
sql = "SELECT sta_id, pm25, ST_Transform(geom, 32647) as geom FROM {tb}".format(
    tb=shp_out)

cmd = '''ogr2ogr -overwrite -f \"ESRI Shapefile\" {shp_name}.shp PG:"host={host} user={username} dbname={db} password={password}" -sql "{sql}"'''.format(
    shp_name=shp_out, host=dbServer, username=dbUser, db=dbName, password=dbPW, sql=sql)
os.system(cmd)

idw = gdal.Grid("idw.tif", "v_pcd_aqi_d1.shp", zfield="pm25",
                algorithm="invdist")
idw = None
