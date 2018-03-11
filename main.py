from ClimateDataModel import ClimateData
from SI7021 import SI7021
import datetime
import dateutil.relativedelta as relativedelta
import peewee
from playhouse.shortcuts import model_to_dict
import paho.mqtt.publish as publish
import json
import math


def roundToPointFive(number):
    return round(number * 2) / 2


def json_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


if __name__ == "__main__":

    config = json.load(open('config.json'))

    try:
        ClimateData._meta.database = peewee.SqliteDatabase(config['database'])
        ClimateData.create_table()
    except peewee.OperationalError:
        pass

    si7021 = SI7021()
    si7021.read()

    last = ClimateData.select().order_by(ClimateData.timestamp.desc()).first()

    if last and last.temperature == si7021.temperature and last.humidity == si7021.humidity:
        exit()

    current = ClimateData.create(
        timestamp=datetime.datetime.now(),
        temperature=roundToPointFive(si7021.temperature),
        humidity=roundToPointFive(si7021.humidity)
    )
    current.save()

    publish.single(config['mqtt_current_topic'],
                   payload=json.dumps(model_to_dict(
                       current), default=json_datetime),
                   hostname=config['mqtt_host'],
                   port=int(config['mqtt_port']),
                   retain=True)

    month = datetime.datetime.now() - relativedelta.relativedelta(months=1)
    query = ClimateData.select().where(ClimateData.timestamp >
                                       month).order_by(ClimateData.id).execute()

    month = []
    for dataset in query:
        month.append(model_to_dict(dataset))

    publish.single(config['mqtt_month_topic'],
                   payload=json.dumps(month, default=json_datetime),
                   hostname=config['mqtt_host'],
                   port=int(config['mqtt_port']),
                   retain=True)
