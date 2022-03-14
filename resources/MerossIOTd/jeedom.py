import asyncio
import json
import logging
import socketserver
import threading
import time
from typing import List

import requests
from meross_iot.controller.device import BaseDevice
from meross_iot.controller.mixins.consumption import ConsumptionXMixin
from meross_iot.model.enums import Namespace, OnlineStatus
from meross_iot.model.push.generic import GenericPushNotification

from meross import MerossCoordinator


# Envoi vers Jeedom ------------------------------------------------------------
class JeedomCallback:
    def __init__(self, apikey, url):
        self.apikey = apikey
        self.url = url
        self.messages = []
        self._stop = False
        self.t = threading.Thread(target=self.run)
        self.t.setDaemon(True)
        self.t.start()

    def stop(self):
        self._stop = True

    def send(self, message):
        self.messages.append(message)
        logging.debug('Nouveau message : {}'.format(message))
        logging.debug('Nombre de messages : {}'.format(len(self.messages)))

    def send_now(self, message):
        return self._request(message)

    def run(self):
        while not self._stop:
            while self.messages:
                m = self.messages.pop(0)
                try:
                    self._request(m)
                except Exception as error:
                    logging.error('Erreur envoie requête à jeedom {}'.format(error))
            time.sleep(0.5)

    def _request(self, m):
        response = None
        logging.debug('Envoie à jeedom :  {}'.format(m))
        r = requests.post('{}?apikey={}'.format(self.url, self.apikey), data=json.dumps(m),
                          verify=False)
        if r.status_code != 200:
            logging.error(
                'Erreur envoie requête à jeedom, return code {} - {}'.format(r.status_code,
                                                                             r.reason))
        else:
            response = r.json()
            logging.debug('Réponse de jeedom :  {}'.format(response))
        return response

    def test(self):
        logging.debug('Envoi un test à jeedom')
        r = self.send_now({'action': 'test'})
        if not r or not r.get('success'):
            logging.error('Erreur envoi à jeedom')
            return False
        return True

    async def event_handler(self, push: GenericPushNotification, devices: List[BaseDevice]):
        logging.debug("Event : {}".format(push.namespace))
        if push.namespace == Namespace.CONTROL_TOGGLEX:
            for index, device in devices:
                self.send(
                    {'action': 'switch', 'uuid': device.uuid,
                     'channel': push.raw_data['togglex'][index].channel,
                     'status': int(push.raw_data['togglex'][index].onoff)})

        elif push.namespace == Namespace.SYSTEM_ONLINE:
            for index, device in devices:
                self.send({'action': 'online', 'uuid': device.uuid,
                           'status': push.raw_data['online'][index].status})

        # Not sure...
        elif push.namespace == Namespace.GARAGE_DOOR_STATE:
            for index, device in devices:
                self.send({'action': 'door', 'uuid': device.uuid,
                           'channel': push.raw_data['door'][index].channel,
                           'status': push.raw_data['door'][index].door_state})

        elif push.namespace == Namespace.CONTROL_BIND:
            for index, device in devices:
                self.send(
                    {'action': 'bind', 'uuid': device.uuid, 'data': push.raw_data['bind'][index]})

        elif push.namespace == Namespace.CONTROL_UNBIND:
            for device in devices:
                self.send({'action': 'unbind', 'uuid': device.uuid})

        # TODO
        # HUMIDIFIER
        # elif eventobj.event_type == MerossEventType.HUMIDIFIER_LIGHT_EVENT:
        #     self.send(
        #         {'action': 'hlight', 'uuid': eventobj.device.uuid, 'channel': eventobj.channel,
        #          'status': int(eventobj.is_on), 'rgb': int(to_rgb(eventobj.rgb)),
        #          'luminance': eventobj.luminance})
        # elif eventobj.event_type == MerossEventType.HUMIDIFIER_SPRY_EVENT:
        #     self.send(
        #         {'action': 'hspray', 'uuid': eventobj.device.uuid, 'channel': eventobj.channel,
        #          'status': int(eventobj.spry_mode.value)})
        # elif eventobj.event_type == MerossEventType.CLIENT_CONNECTION:
        #    self.send({'action': 'connect', 'status': eventobj.status.value})


# Reception de Jeedom ----------------------------------------------------------
class JeedomHandler(socketserver.BaseRequestHandler):

    def __init__(self, meross_coordinator: MerossCoordinator, api_key):
        self._meross_coordinator = meross_coordinator
        self._api_key = api_key
        super().__init__(self)

    def handle(self):
        # self.request is the TCP socket connected to the client
        data = self.request.recv(1024)
        logging.info("Message received in socket")
        message = json.loads(data.decode())
        lmessage = dict(message)
        del lmessage['apikey']
        logging.info(lmessage)
        if message.get('apikey') != self._api_key:
            logging.error("Invalid apikey from socket : {}".format(data))
            return
        response = {'result': None, 'success': True}
        action = message.get('action')
        args = message.get('args')
        if hasattr(self._meross_coordinator, action):
            func = getattr(self._meross_coordinator, action)
            response['result'] = func
            if callable(response['result']):
                response['result'] = response['result'](*args)
        logging.info(response)
        self.request.sendall(json.dumps(response).encode())
