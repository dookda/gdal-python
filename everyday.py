import schedule
import time


def bedtime():
    print("test")


schedule.every().day.at("21:28").do(bedtime)
while True:
    schedule.run_pending()
    time.sleep(1)
