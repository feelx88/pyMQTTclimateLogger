import peewee


class ClimateData(peewee.Model):
    """
    ORM model of the ClimateData table
    """
    timestamp = peewee.DateTimeField()
    temperature = peewee.IntegerField()
    humidity = peewee.IntegerField()
