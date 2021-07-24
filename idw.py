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

col = "aqi"
tiffpath = "./tiff_"+col+"/"
shppath = "./shp/"

tbs = ["v_pcd_aqi_d1", "v_pcd_aqi_d2", "v_pcd_aqi_d3",
       "v_pcd_aqi_d4", "v_pcd_aqi_d5", "v_pcd_aqi_d6",
       "v_pcd_aqi_d7", "v_pcd_aqi_d8", "v_pcd_aqi_d9",
       "v_pcd_aqi_d10", "v_pcd_aqi_d11", "v_pcd_aqi_d12",
       "v_pcd_aqi_d13", "v_pcd_aqi_d14"]
for tb in tbs:
    sql = "SELECT sta_id, {col}, ST_Transform(geom, 32647) as geom FROM {tb}".format(
        col=col, tb=tb)

    cmd = '''ogr2ogr -overwrite -f \"ESRI Shapefile\" {shppath}{shp_name}.shp PG:"host={host} user={username} dbname={db} password={password}" -sql "{sql}"'''.format(
        shppath=shppath, shp_name=tb, host=dbServer, username=dbUser, db=dbName, password=dbPW, sql=sql)
    os.system(cmd)

    out = tiffpath+col+"_"+tb+".tif"
    print(out)

    idw = gdal.Grid(out, shppath+tb+".shp", zfield=col,
                    algorithm="invdist")
    idw = None

    cmd = "gdal_contour -a elev {out} {shppath}{shp_name}_contour.shp -i 5.0".format(
        out=out, shppath=shppath, shp_name=tb)
    os.system(cmd)
