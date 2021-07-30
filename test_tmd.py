import requests
import psycopg2 as pg2


dbServer = "119.59.125.134"
dbName = "data"
dbUser = "postgres"
dbPW = "Pgis@rti2dss@2020"
port = "5432"

conn = pg2.connect(database=dbName, user=dbUser,
                   password=dbPW, host=dbServer, port=port)
# cursor = conn.cursor()

# sql = "SELECT * FROM ews_3hr"


# cursor.execute(sql)
# data = cursor.fetchall()
# print(data)

# conn.close()

# url = "https://data.tmd.go.th/nwpapi/v1/forecast/location/daily"

# headers = {
#     'accept': "application/json",
#     'authorization': "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjE1ZjUwYzM0ODY2ODdlY2Y1ZmE0NmZkMGFmZGVhYTJlYWQ3ZGMyNjcyZTJhMjg5MDEzZTlmYmU2NTI5YWVkZDUzYWQwMjE2ZWUyNTY3MTUzIn0.eyJhdWQiOiIyIiwianRpIjoiMTVmNTBjMzQ4NjY4N2VjZjVmYTQ2ZmQwYWZkZWFhMmVhZDdkYzI2NzJlMmEyODkwMTNlOWZiZTY1MjlhZWRkNTNhZDAyMTZlZTI1NjcxNTMiLCJpYXQiOjE2Mjc2MTMyMjgsIm5iZiI6MTYyNzYxMzIyOCwiZXhwIjoxNjU5MTQ5MjI4LCJzdWIiOiIxNTUwIiwic2NvcGVzIjpbXX0.ottFakPY4sW8Z1lNf2XKe43M3USH29KKau0sAPlT9X4lekxumvcpSOe8jrbj-Bp0vDdJfB6psfoklYEzAuLC6dCrPoFSM2dTjkAd6bNjc-2nCFxV6hwKiwXxvYXCRS1MuZTRssZws1HOAGgy1ewfbtjKIvVTqNKa0dy6TXDwkdRxvAszyikIVCYSHxQy1buI8NzCvppIAN5oKb1AJgEhYWjB9mXv9xjoyMBoo3Fo_Bjn4JqJjDtQMdWrVaamTVHSwaZKFZmvHJMnNRYgMbVHZo6tYi6JCqtqoDpYCvrdKXYLoa7_JsXqJsjEMF0UPRa9b2zUFq5WJ5fUaB0D5j2HTwNK4LvxsWNNJgZJCChMQBCgBAT9GDD_-aqTnMnAyISVegtxEYn8CnVjjJtP2Jjuvrvt_1fSru1PesPe3GzZtJ0PwUC3pNbyMueKzatDKEpDNn641dWK2nISGaBcrGaPKzRDhX7oPMJwnb5J0tegkwRffigpcrTvGwBe-4DV2vBtE3OxcpWEACGT36_DovUARnn3nuA_FL-Qsbi5DOR85SUvjmmvyf2Asac2Yx3Cx5NyYOuUGCWpkBI52tTrwcwo8OMCS5jEiQEpJ0uPlBVrG-UfESW6mSXAOEfhcglEWoXz7t-YVYObDCCSlhC9jCQs6yjtE7GkyPNh48UFpFWpPqE",
# }

# response = requests.request("GET", url, headers=headers)
# print(response.text)

url = "https://data.tmd.go.th/nwpapi/v1/forecast/location/daily/at"

querystring = {"lat": "13.10", "lon": "100.10", "fields": "tc_max,rh,rain",
               "date": "2021-07-30", "duration": "7"}

headers = {
    'accept': "application/json",
    'authorization': "Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImp0aSI6IjM0ZDhkNWVhMzQyNGQxMDMxZWM3Nzc0MzFkYWMzODMwOTQ3MDViNTUxMjMxZTVlNDM0MzFiYjExYzc2MWZlODExNTI5MWQxNTE2YmYzZTM5In0.eyJhdWQiOiIyIiwianRpIjoiMzRkOGQ1ZWEzNDI0ZDEwMzFlYzc3NzQzMWRhYzM4MzA5NDcwNWI1NTEyMzFlNWU0MzQzMWJiMTFjNzYxZmU4MTE1MjkxZDE1MTZiZjNlMzkiLCJpYXQiOjE2Mjc2MDcwMjYsIm5iZiI6MTYyNzYwNzAyNiwiZXhwIjoxNjU5MTQzMDI2LCJzdWIiOiIzNjciLCJzY29wZXMiOltdfQ.BnMxqqk0zaIemodcQbHMkqx7M4PGxCRW6ZdpW4-EJdMp3D-p-MzZh1fFg25Eq3lpZIFI9EldaLIOQHb-L6cMblVrNtZSDupbP9y10O0fEjeXg-yeiv3LR8zA7Mm-LOw9qO6QuzsCOGjJkGVnHq7-lSLVZGPtaPiAhYJ2XHno4Iv2e_0UY-m_taqvZ-8GLG-Fgyr_4DHrUFAE9O4HLT-lJGh0Cur6v_epUnfYpkHTrr6DC4_36OGvhjNMj2zHlEUgRWFJBSUACWNpdWLfsVldxM3h54jL5SQ1Cgs_L2_GIcPmVvEcBH0ID8X5hM_nl0YD_x7Kpiam3uBnJTt1TuI4uNbRleLmG6na4isNq2SktUh9HL2X4J2i-qJYNNjTYqPpJ2cEnGvr-aFUvjEmQxRLdzf7h8_OzJRUAPhB9LJ0N9aL4f5NGWsxYx_2ktrc1-ap08MvrhIWpOe8RzUvdmw8EO39JQR8z6iDcx-xMbKC2OQ8LNBOVXcFhth8Q-wexphIwqgH-tj6xT8OSoDZreGYxfqPAPH2z22JBPQlYRRYpYGwUFq7iL15YbH4p24ydwRrDjstEkrFW3OWHcbIsAMDYd-sULeLK-bhWgvoLRva7vXXYBzZpfTQAw1ns_-fnrv43EkrcsRdol1CRwtH8UqyikL4TGZD01n6eJCmcIiNKfw",
}

# response = requests.request("GET", url, headers=headers)


response = requests.request("GET", url, headers=headers, params=querystring)

print(response.text)
