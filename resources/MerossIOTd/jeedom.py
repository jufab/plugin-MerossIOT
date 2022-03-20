import json
import logging
import threading
import time
from typing import List

import requests
from meross_iot.controller.device import BaseDevice
from meross_iot.model.enums import Namespace
from meross_iot.model.push.generic import GenericPushNotification

logger = logging.getLogger()


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
        logger.debug('Nouveau message : {}'.format(message))
        logger.debug('Nombre de messages : {}'.format(len(self.messages)))

    def send_now(self, message):
        return self._request(message)

    def run(self):
        while not self._stop:
            while self.messages:
                m = self.messages.pop(0)
                try:
                    self._request(m)
                except Exception as error:
                    logger.error('Erreur envoie requête à jeedom {}'.format(error))
            time.sleep(0.5)

    def _request(self, m):
        response = None
        logger.debug('Envoie à jeedom :  {}'.format(m))
        r = requests.post('{}?apikey={}'.format(self.url, self.apikey), data=json.dumps(m),
                          verify=False)
        if r.status_code != 200:
            logger.error(
                'Erreur envoie requête à jeedom, return code {} - {}'.format(r.status_code,
                                                                             r.reason))
        else:
            response = r.json()
            logger.debug('Réponse de jeedom :  {}'.format(response))
        return response

    def test(self):
        logger.debug('Envoi un test à jeedom')
        r = self.send_now({'action': 'test'})
        if not r or not r.get('success'):
            logger.error('Erreur envoi à jeedom')
            return False
        return True

    async def event_handler(self, push: GenericPushNotification, devices: List[BaseDevice], device_internal_id: str):
        if push.namespace == Namespace.CONTROL_TOGGLEX:
            for index, device in enumerate(devices):
                self.send(
                    {'action': 'switch', 'uuid': device.uuid,
                     'channel': push.raw_data['togglex'][index]['channel'],
                     'status': int(push.raw_data['togglex'][index]['onoff'])
                     })

        elif push.namespace == Namespace.SYSTEM_ONLINE:
            for index, device in enumerate(devices):
                self.send({'action': 'online', 'uuid': device.uuid,
                           'status': push.raw_data['online'][index]['status']
                           })

        # Not sure...
        elif push.namespace == Namespace.GARAGE_DOOR_STATE:
            for index, device in enumerate(devices):
                self.send({'action': 'door', 'uuid': device.uuid,
                           'channel': push.raw_data['door'][index]['channel'],
                           'status': push.raw_data['door'][index]['door_state']})

        elif push.namespace == Namespace.CONTROL_BIND:
            for index, device in enumerate(devices):
                self.send(
                    {'action': 'bind', 'uuid': device.uuid, 'data': push.raw_data['bind'][index]})

        elif push.namespace == Namespace.CONTROL_UNBIND:
            for device in enumerate(devices):
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
