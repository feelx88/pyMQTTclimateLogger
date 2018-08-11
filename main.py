from ClimateDataModel import ClimateData
from SI7021 import SI7021
import datetime
import dateutil.relativedelta as relativedelta
import peewee
from playhouse.shortcuts import model_to_dict
import paho.mqtt.publish as publish
import json
import math


def json_datetime(obj):
    if isinstance(obj, datetime.datetime):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def mqttPublish(config, topicKey, payload):
    publish.single(config[topicKey],
                   payload=json.dumps(payload, default=json_datetime),
                   hostname=config['mqtt_host'],
                   port=int(config['mqtt_port']),
                   retain=True)


if __name__ == "__main__":

    config = json.load(open('config.json'))

    try:
        ClimateData._meta.database = peewee.SqliteDatabase(config['database'])
        ClimateData.create_table()
    except peewee.OperationalError:
        pass

    si7021 = SI7021()
    si7021.read()

    temperature = int(round(si7021.temperature)) * 10
    humidity = int(round(si7021.humidity / 10)) * 10 * 10

    last = ClimateData.select().order_by(ClimateData.timestamp.desc()).first()

    if last and last.temperature == temperature and last.humidity == humidity:
        exit()

    current = ClimateData.create(
        timestamp=datetime.datetime.now(),
        temperature=temperature,
        humidity=humidity
    )
    current.save()

    mqttPublish(config, 'mqtt_current_topic', model_to_dict(current))

    month = datetime.datetime.now() - relativedelta.relativedelta(months=1)
    query = ClimateData.select().where(ClimateData.timestamp >
                                       month).order_by(ClimateData.id).execute()

    month = []
    for dataset in query:
        month.append(model_to_dict(dataset))

    mqttPublish(config, 'mqtt_month_topic', month)

    day = datetime.datetime.now() - relativedelta.relativedelta(days=1)
    query = ClimateData.select().where(ClimateData.timestamp >
                                       day).order_by(ClimateData.id).execute()

    day = []
    for dataset in query:
        day.append(model_to_dict(dataset))

    mqttPublish(config, 'mqtt_day_topic', day)
