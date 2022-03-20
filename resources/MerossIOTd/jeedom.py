import asyncio
import json
import logging
from typing import List

import httpx
from meross_iot.controller.device import BaseDevice
from meross_iot.model.enums import Namespace
from meross_iot.model.push.generic import GenericPushNotification

logger = logging.getLogger('jeedom')


# Envoi vers Jeedom ------------------------------------------------------------
class JeedomCallback:
    def __init__(self, apikey, url):
        self.apikey = apikey
        self.url = url

    async def send_message(self, message):
        async with httpx.AsyncClient() as client:
            response = None
            logger.debug(f'Envoie à jeedom :  {message}')
            r = await client.post(f'{self.url}?apikey={self.apikey}', data=json.dumps(message))
            if r.status_code != 200:
                logger.error(
                    f'Erreur envoie requête à jeedom, return code {r.status_code} - {r.reason}')
            else:
                response = r.json()
                logger.debug(f'Réponse de jeedom :  {response}')
            return response

    async def test(self):
        logger.debug('Envoi un test à jeedom')
        r = await self.send_message({'action': 'test'})
        if not r or not r.get('success'):
            logger.error('Erreur envoi à jeedom')
            return False
        return True

    async def event_handler(self, push: GenericPushNotification, devices: List[BaseDevice],
                            device_internal_id: str):
        logger.debug(
            f'Reception de {push.namespace} et {push.raw_data} pour la liste d appareils {devices}')
        if push.namespace == Namespace.CONTROL_TOGGLEX:
            return await asyncio.gather(
                *(self.send_message({'action': 'switch', 'uuid': device.uuid,
                                     'channel': push.raw_data['togglex'][index]['channel'],
                                     'status': int(
                                         push.raw_data['togglex'][index]['onoff'])})
                  for index, device in enumerate(devices)))

        elif push.namespace == Namespace.SYSTEM_ONLINE:
            return await asyncio.gather(
                *(self.send_message({'action': 'online', 'uuid': device.uuid,
                                     'status': push.raw_data['online'][index]['status']})
                  for index, device in enumerate(devices)))

        # Not sure...
        elif push.namespace == Namespace.GARAGE_DOOR_STATE:
            return await asyncio.gather(
                *(self.send_message({'action': 'door', 'uuid': device.uuid,
                                     'channel': push.raw_data['door'][index]['channel'],
                                     'status': push.raw_data['door'][index]['door_state']})
                  for index, device in enumerate(devices)))

        elif push.namespace == Namespace.CONTROL_BIND:
            return await asyncio.gather(
                *(self.send_message(
                    {'action': 'bind', 'uuid': device.uuid, 'data': push.raw_data['bind'][index]})
                    for index, device in enumerate(devices)))

        elif push.namespace == Namespace.CONTROL_UNBIND:
            return await asyncio.gather(
                *(self.send_message({'action': 'unbind', 'uuid': device.uuid}) for index, device in
                  enumerate(devices)))

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
