import requests
import psycopg2 as pg2
from datetime import date
import os
from osgeo import gdal
from osgeo import ogr
import schedule
import time


dbServer = "150.95.89.49"
dbName = "eec"
dbUser = "postgres"
dbPW = "Eec-MIS2564db"
port = "5432"

conn = pg2.connect(database=dbName, user=dbUser,
                   password=dbPW, host=dbServer, port=port)

conn.autocommit = True
cursor = conn.cursor()
token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjM0ZDhkNWVhMzQyNGQxMDMxZWM3Nzc0MzFkYWMzODMwOTQ3MDViNTUxMjMxZTVlNDM0MzFiYjExYzc2MWZlODExNTI5MWQxNTE2YmYzZTM5In0.eyJhdWQiOiIyIiwianRpIjoiMzRkOGQ1ZWEzNDI0ZDEwMzFlYzc3NzQzMWRhYzM4MzA5NDcwNWI1NTEyMzFlNWU0MzQzMWJiMTFjNzYxZmU4MTE1MjkxZDE1MTZiZjNlMzkiLCJpYXQiOjE2Mjc2MDcwMjYsIm5iZiI6MTYyNzYwNzAyNiwiZXhwIjoxNjU5MTQzMDI2LCJzdWIiOiIzNjciLCJzY29wZXMiOltdfQ.BnMxqqk0zaIemodcQbHMkqx7M4PGxCRW6ZdpW4-EJdMp3D-p-MzZh1fFg25Eq3lpZIFI9EldaLIOQHb-L6cMblVrNtZSDupbP9y10O0fEjeXg-yeiv3LR8zA7Mm-LOw9qO6QuzsCOGjJkGVnHq7-lSLVZGPtaPiAhYJ2XHno4Iv2e_0UY-m_taqvZ-8GLG-Fgyr_4DHrUFAE9O4HLT-lJGh0Cur6v_epUnfYpkHTrr6DC4_36OGvhjNMj2zHlEUgRWFJBSUACWNpdWLfsVldxM3h54jL5SQ1Cgs_L2_GIcPmVvEcBH0ID8X5hM_nl0YD_x7Kpiam3uBnJTt1TuI4uNbRleLmG6na4isNq2SktUh9HL2X4J2i-qJYNNjTYqPpJ2cEnGvr-aFUvjEmQxRLdzf7h8_OzJRUAPhB9LJ0N9aL4f5NGWsxYx_2ktrc1-ap08MvrhIWpOe8RzUvdmw8EO39JQR8z6iDcx-xMbKC2OQ8LNBOVXcFhth8Q-wexphIwqgH-tj6xT8OSoDZreGYxfqPAPH2z22JBPQlYRRYpYGwUFq7iL15YbH4p24ydwRrDjstEkrFW3OWHcbIsAMDYd-sULeLK-bhWgvoLRva7vXXYBzZpfTQAw1ns_-fnrv43EkrcsRdol1CRwtH8UqyikL4TGZD01n6eJCmcIiNKfw"
url = "https://data.tmd.go.th/nwpapi/v1/forecast/location/daily/at"
headers = {
    'accept': "application/json",
    'authorization': "Bearer {token}".format(token=token),
}
# print(headers)


def clearTb():
    sql = "DELETE FROM tmd_forecast"
    cursor.execute(sql)


def selectSampling():
    sql = "SELECT lat, lon, sta FROM eec_sampling_4326"
    cursor.execute(sql)
    result = cursor.fetchall()
    return result


def getApi(staLocation):
    today = date.today()
    for r in staLocation:
        querystring = {"lat": str(r[0]), "lon": str(r[1]), "fields": "tc_max,tc_min,rh,rain",
                       "date": str(today), "duration": "8"}
        print(querystring)
        response = requests.request(
            "GET", url, headers=headers, params=querystring)
        dicts = eval(response.text)

        for dict in dicts:
            for d in dicts[dict]:
                for i in d["forecasts"]:
                    sql = "INSERT INTO tmd_forecast(sta,dt,rain,rh,tc_max,tc_min, geom)VALUES('{sta}','{dt}',{rain},{rh},{tc_max},{tc_min}, ST_GeomFromText('POINT({lon} {lat})', 4326))".format(
                        sta=r[2], dt=i["time"], rain=i["data"]["rain"], rh=i["data"]["rh"], tc_max=i["data"]["tc_max"], tc_min=i["data"]["tc_min"], lon=r[1], lat=r[0])
                    cursor.execute(sql)
                    print(sql)


def interp(shp, name):
    out = "./tiff_forecast/{name}.tif".format(name=name)
    print(out)
    gdal.Grid(out, shp, zfield='rain',
              algorithm="invdist:power=3")


def createSHP():
    i = 1
    while i <= 7:
        sql = "SELECT gid, rain, TO_CHAR(dt, 'DD-MM-YYYY') as dt, geom FROM tmd_forecast WHERE dt = CURRENT_DATE + {i}".format(
            i=i)
        print(sql)
        shppath = "./shp_forecast/"
        shpname = "tmd_nextday{d}".format(d=i)
        outshp = "{shppath}{shpname}.shp".format(
            shppath=shppath, shpname=shpname)
        cmd = '''ogr2ogr -overwrite -f \"ESRI Shapefile\" {outshp} PG:"host={host} user={username} dbname={db} password={password}" -sql "{sql}"'''.format(
            outshp=outshp, host=dbServer, username=dbUser, db=dbName, password=dbPW, sql=sql)
        os.system(cmd)
        interp(outshp, shpname)
        i += 1


def runSched():
    clearTb()
    staLocation = selectSampling()
    print(staLocation)
    getApi(staLocation)
    createSHP()

    conn.commit()
    conn.close()


if __name__ == "__main__":
    schedule.every().day.at("09:10").do(runSched)
    while True:
        schedule.run_pending()
        time.sleep(1)
