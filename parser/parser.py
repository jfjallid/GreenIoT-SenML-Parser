import simplejson as json
from elasticsearch import Elasticsearch
from elasticsearch import helpers
from flask import Flask, request
from flask_restful import Api, Resource
from threading import Thread
from dateutil import parser
import logging
from senml import senml
import os

elastic_host = os.environ['ELASTIC_HOST']
if not elastic_host:
    elastic_host = 'localhost'
elastic_port = os.environ['ELASTIC_PORT']
if not elastic_port:
    elastic_port = 9200
es = Elasticsearch([{'host': elastic_host, 'port': elastic_port}])


def parse_senml(data):
    try:
        documents = []
        ml = senml.SenMLDocument.from_json(data['data'])
        logger.debug('Data fit in SenMLDocument!')

        for measurement in ml.measurements:
            resolved = measurement.to_absolute(base=ml.base).to_json()

            doc = {}
            if 't' in resolved:
                t = resolved.get('t')
                epoch_time_now = parser.parse(data['timestamp']).timestamp()
                if 268435456 <= t <= epoch_time_now:  # Absolute time cut-off point
                    resolved['t'] = float(t)
                else:
                    # A timestamp in the future is not relevant for sensor measurements.
                    del resolved['t']

            resolved['timestamp'] = data['timestamp']
            resolved['uuid'] = data['uuid']
            doc['_index'] = 'measurements'
            doc['_type'] = '_doc'
            doc['_source'] = resolved
            doc['pipeline'] = 'dailyindex'
            logger.debug(doc)
            documents.append(doc)

        logger.debug(f'Done parsing message into {len(documents)} documents!')
        return documents
    except Exception as e:
        logger.debug(
            f'Could not parse the following document as SenML: {data}, got exception: {e}'
        )


def parse_json_document(data):
    try:
        doc = dict()
        doc['_index'] = 'measurements'
        doc['_type'] = '_doc'
        doc['_source'] = data['data']
        doc['_source']['timestamp'] = data['timestamp']
        doc['_source']['uuid'] = data['uuid']
        doc['pipeline'] = 'dailyindex'
        logger.info(str(doc))
        return doc

    except Exception as e:
        logger.debug(f'Could not parse object: {data}, got exception: {e}')


def parse(json_data):
    if json_data.get('data'):

        if isinstance(json_data['data'], list):
            logger.debug('Trying to parse as SenML!')
            """Try to parse as SenML"""
            documents = parse_senml(json_data)
            if documents:
                logger.debug('Parsed message as SenML!')
                try:
                    helpers.bulk(es, documents)
                except Exception as e:
                    logger.error(str(e))

                return
            logger.info(f'No SenML documents as a result of parsing the message: {json_data}')
        else:
            logger.debug('Trying to parse as json document')
            document = parse_json_document(json_data)
            if document:
                logger.debug('Parsed message as json document')
                documents = [document]
                try:
                    helpers.bulk(es, documents)
                except Exception as e:
                    logger.error(str(e))
                return

        logger.info(f"Couldn't parse message! {json_data}")
    else:
        logger.debug(f'Message not from tagger, missing "data" field! {json_data}')


class Parser(Resource):
    def post(self):
        json_data = json.loads(request.get_json())
        Thread(target=parse, args=(json_data,)).start()
        return 'OK', 200


logging.basicConfig(level=logging.INFO)  # Set this to logging.DEBUG to enable debugging
logger = logging.getLogger('Parser')
logging.getLogger('elasticsearch').setLevel(logging.ERROR)
logging.getLogger('werkzeug').setLevel(logging.ERROR)
app = Flask(__name__)
api = Api(app)
api.add_resource(Parser, '/parse/')

if __name__ == '__main__':
    app.run(debug=True)
