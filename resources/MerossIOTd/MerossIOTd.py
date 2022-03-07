#!/usr/bin/env python3
import argparse
import asyncio
import json
import logging
import os
import signal
import socketserver
import sys
import threading
import time
from datetime import datetime
from typing import List

import requests
from meross_iot.controller.device import BaseDevice
from meross_iot.controller.mixins.consumption import ConsumptionXMixin
from meross_iot.controller.mixins.electricity import ElectricityMixin
from meross_iot.http_api import MerossHttpClient
from meross_iot.manager import MerossManager
from meross_iot.model.enums import OnlineStatus, Namespace
from meross_iot.model.push.generic import GenericPushNotification


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

    # def event_handler(self, eventobj):
    async def event_handler(self, push: GenericPushNotification, devices: List[BaseDevice],
                            meross_manager):
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
    def handle(self):
        # self.request is the TCP socket connected to the client
        data = self.request.recv(1024)
        logging.info("Message received in socket")
        message = json.loads(data.decode())
        lmessage = dict(message)
        del lmessage['apikey']
        logging.info(lmessage)
        if message.get('apikey') != _apikey:
            logging.error("Invalid apikey from socket : {}".format(data))
            return
        response = {'result': None, 'success': True}
        action = message.get('action')
        args = message.get('args')
        if hasattr(self, action):
            func = getattr(self, action)
            response['result'] = func
            if callable(response['result']):
                response['result'] = response['result'](*args)
        logging.info(response)
        self.request.sendall(json.dumps(response).encode())

    async def setOn(self, uuid, channel=0):
        device = _meross_manager.find_devices(device_uuids=uuid)[0]
        if device is not None:
            if device.abilities[Namespace.GARAGE_DOOR_STATE]:
                await device.async_close(channel=int(channel))
            else:
                await device.async_turn_on(channel=int(channel))
            return ''
        else:
            return 'Unknow device'

    async def setOff(self, uuid, channel=0):
        device = _meross_manager.find_devices(device_uuids=uuid)[0]
        if device is not None:
            if device.abilities[Namespace.GARAGE_DOOR_STATE]:
                await device.async_open(channel=int(channel))
            else:
                await device.async_turn_off(channel=int(channel))
            return
        else:
            return 'Unknow device'

    async def setLumi(self, uuid, lumi_int):
        device = _meross_manager.find_devices(device_uuids=uuid)[0]
        if device is not None:
            await device.async_set_light_color(luminance=int(lumi_int))
            return
        else:
            return 'Unknow device'

    async def setTemp(self, uuid, temp_int, lumi=-1):
        device = _meross_manager.find_devices(device_uuids=uuid)[0]
        if device is not None:
            await device.async_set_light_color(temperature=temp_int, luminance=lumi)
            return
        else:
            return 'Unknow device'

    async def setRGB(self, uuid, rgb_int, lumi=-1):
        device = _meross_manager.find_devices(device_uuids=uuid)[0]
        if device is not None:
            await device.async_set_light_color(rgb=int(rgb_int), luminance=lumi)
            return
        else:
            return 'Unknow device'

    async def setSpray(self, uuid, smode=0):
        # device = meross_manager.get_device_by_uuid(uuid)
        # if device is not None:
        #     if smode == '1':
        #         res = device.set_spray_mode(spray_mode=SprayMode.CONTINUOUS)
        #     elif smode == '2':
        #         res = device.set_spray_mode(spray_mode=SprayMode.INTERMITTENT)
        #     else:
        #         res = device.set_spray_mode(spray_mode=SprayMode.OFF)
        #     return res
        # else:
        #     return 'Unknow device'
        return 'Not Implemented Yet'

    async def syncOneMeross(self, device):
        await device.async_update()
        d = dict({
            'name': device.name,
            'uuid': device.uuid,
            'famille': str(device.__class__.__name__),
            'online': device.online_status == OnlineStatus.ONLINE,
            'type': device.type,
            'ip': '',
            'mac': ''
        })
        # Hors ligne : fin
        if device.online_status != OnlineStatus.ONLINE:
            return d
        # En Ligne Seulement
        data = device.abilities
        d['values'] = {}
        # Nom Canaux
        onoff = [device.name]
        for x in device._channels:
            try:
                onoff.append(x['name'])
            except:
                pass
        d['onoff'] = onoff
        # Valeur Canaux
        switch = []
        try:
            for x in device._channels:
                switch.append(device.is_on(channel=x.index))
        except:
            try:
                for x in device._channels:
                    switch.append(device.get_light_is_on(channel=x.index))
            except:
                pass
        d['values']['switch'] = switch
        # IP
        # try:
        #    d['ip'] = data['all']['system']['firmware']['innerIp']
        # except:
        #    pass
        # MAC
        # try:
        #    d['mac'] = data['all']['system']['hardware']['macAddress']
        # except:
        #    pass
        # Puissance
        if data[Namespace.CONTROL_ELECTRICITY]:
            d['elec'] = True
            electricity = await device.async_get_instant_metrics()
            d['values']['power'] = electricity.power
            d['values']['current'] = electricity.current
            d['values']['voltage'] = electricity.voltage
        else:
            d['elec'] = False
        # Consommation
        if data[Namespace.CONTROL_CONSUMPTIONX] or data[Namespace.CONTROL_CONSUMPTION]:
            d['conso'] = True
            l_conso = await device.async_get_daily_power_consumption()
            d['values']['conso_totale'] = 0
            today = datetime.today().strftime("%Y-%m-%d")
            for c in l_conso:
                dateconso = c['date'].strftime("%Y-%m-%d")
                if dateconso == today:
                    d['values']['conso_totale'] = c['value']
        else:
            d['conso'] = False
        # Lumiere
        if data[Namespace.CONTROL_LIGHT]:
            d['light'] = True
            d['lumin'] = device.get_supports_luminance()
            d['tempe'] = device.get_supports_temperature()
            d['isrgb'] = device.get_supports_rgb()
            if d['lumin']:
                d['values']['lumival'] = device.get_luminance()
            if d['tempe']:
                d['values']['tempval'] = device.get_color_temperature()
            if d['isrgb']:
                d['values']['rgbval'] = device.get_rgb_color()
        else:
            d['light'] = False
            d['lumin'] = False
            d['tempe'] = False
            d['isrgb'] = False
        # HUMIDIFIER
        if data[Namespace.CONTROL_SPRAY]:
            d['spray'] = True
            # d['values']['spray'] = device.get_spray_mode().value
        else:
            d['spray'] = False
        # Fini
        return d

    async def getMerossConso(self, device):
        d = dict({'conso_totale': 0})
        try:
            l_conso = await device.async_get_daily_power_consumption()
            today = datetime.today().strftime("%Y-%m-%d")
            for c in l_conso:
                dateconso = c['date'].strftime("%Y-%m-%d")
                if dateconso == today:
                    d['conso_totale'] = c['value']
        finally:
            return d

    def syncMeross(self):
        d_devices = {}
        logging.info("Début de synchro global")
        logging.info('[loop-Elec] is meross_manager OK : {}'.format(_meross_manager is not None))
        devices = _meross_manager.find_devices()
        logging.info("liste des devices : {}".format(devices))
        for num in range(len(devices)):
            device = devices[num]
            d = asyncio.run(self.syncOneMeross(device))
            uuid = device.uuid
            d_devices[uuid] = d
        return d_devices

    def syncDevice(self, uuid):
        device = _meross_manager.find_devices(device_uuids=uuid)[0]
        return asyncio.run(self.syncOneMeross(device))

    def syncMerossConso(self):
        d_devices = {}
        devices = _meross_manager.find_devices(device_class=ConsumptionXMixin,
                                               online_status=OnlineStatus.ONLINE)
        for num in range(len(devices)):
            device = devices[num]
            d = asyncio.run(self.getMerossConso(device))
            uuid = device.uuid
            d_devices[uuid] = d
        return d_devices


# Les fonctions du daemon ------------------------------------------------------
def convert_log_level(level='error'):
    LEVELS = {'debug': logging.DEBUG,
              'info': logging.INFO,
              'notice': logging.WARNING,
              'warning': logging.WARNING,
              'error': logging.ERROR,
              'critical': logging.CRITICAL,
              'none': logging.NOTSET}
    return LEVELS.get(level, logging.NOTSET)


def handler(signum=None, frame=None):
    logging.debug("Signal %i caught, exiting..." % int(signum))
    shutdown()


def shutdown():
    logging.debug("Arrêt")
    logging.debug("Arrêt Meross Manager")
    updateElec()
    _meross_manager.unregister_push_notification_handler_coroutine(jc.event_handler)
    _meross_manager.close()
    try:
        asyncio.run(_http_api_client.async_logout())
    except:
        pass
    logging.debug("Stop callback server")
    jc.stop()
    logging.debug("Arrêt du démon local")
    server.shutdown()
    logging.debug("Effacement fichier PID " + str(_pidfile))
    if os.path.exists(_pidfile):
        os.remove(_pidfile)
    logging.debug("Effacement fichier socket " + str(_sockfile))
    if os.path.exists(_sockfile):
        os.remove(_sockfile)
    logging.debug("Exit 0")


# ----------------------------------------------------------------------------
async def syncOneElectricity(device):
    try:
        electricity = await device.async_get_instant_metrics()
        d = dict({'power': 0, 'current': 0, 'voltage': 0})
        d['power'] = electricity.power
        d['voltage'] = electricity.voltage
        d['current'] = electricity.current
        return d
    except:
        pass
    finally:
        return False


def UpdateAllElectricity(interval):
    stopped = threading.Event()

    def loop():
        while not stopped.wait(interval):
            e_devices = {}
            try:
                logging.info('[loop-Elec] is meross_manager OK : {}'.format(_meross_manager is not None))
                # Que les appareils ayant l'info electrique
                devices = _meross_manager.find_devices(device_class=ElectricityMixin,
                                                       online_status=OnlineStatus.ONLINE)
                for num in range(len(devices)):
                    device = devices[num]
                    d = asyncio.run(syncOneElectricity(device))
                    if isinstance(d, dict):
                        uuid = device.uuid
                        e_devices[uuid] = d
                # Fin du for
                logging.info('Send Electricity')
                # jc.sendElectricity(e_devices)
                jc.send({'action': 'electricity', 'values': e_devices})
            except:
                pass

    # fin de loop
    threading.Thread(target=loop).start()
    return stopped.set


async def meross_connection(email, password):
    http: MerossHttpClient = await MerossHttpClient.async_from_user_password(
        email=email,
        password=password)
    mm: MerossManager = MerossManager(http_client=http)
    # Register event handlers for the manager...
    mm.register_push_notification_handler_coroutine(jc.event_handler)
    await mm.async_device_discovery()
    return http, mm


# ----------------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument('--muser', help='Compte Meross', default='')
parser.add_argument('--mpswd', help='Mot de passe Meross', default='')
parser.add_argument('--mupdp', help='Fréquence actualisation puissance', type=int, default=30)
parser.add_argument('--callback', help='Jeedom callback', default='http://localhost')
parser.add_argument('--apikey', help='API Key', default='nokey')
parser.add_argument('--loglevel', help='LOG Level', default='error')
parser.add_argument('--pidfile', help='PID File', default='/tmp/MerossIOTd.pid')
parser.add_argument('--socket', help='Daemon socket', default='/tmp/MerossIOTd.sock')
args = parser.parse_args()

FORMAT = '[%(asctime)-15s][%(levelname)s][%(name)s](%(threadName)s) : %(message)s'
logging.basicConfig(level=convert_log_level(args.loglevel), format=FORMAT,
                    datefmt="%Y-%m-%d %H:%M:%S")
urllib3_logger = logging.getLogger('urllib3')
urllib3_logger.setLevel(logging.CRITICAL)

logging.info('Start MerossIOTd')
logging.info('Log level : {}'.format(args.loglevel))
logging.info('Socket : {}'.format(args.socket))
logging.info('PID file : {}'.format(args.pidfile))
logging.info('Apikey : {}'.format(args.apikey))
logging.info('Update Power : {}'.format(args.mupdp))
logging.info('Callback : {}'.format(args.callback))
logging.info('Python version : {}'.format(sys.version))

_pidfile = args.pidfile
_sockfile = args.socket
_apikey = args.apikey

signal.signal(signal.SIGINT, handler)
signal.signal(signal.SIGTERM, handler)

pid = str(os.getpid())
logging.debug("Ecriture du PID " + pid + " dans " + str(args.pidfile))
with open(args.pidfile, 'w') as fp:
    fp.write("%s\n" % pid)

jc = JeedomCallback(args.apikey, args.callback)
if not jc.test():
    sys.exit()

if os.path.exists(args.socket):
    os.unlink(args.socket)

server = socketserver.UnixStreamServer(args.socket, JeedomHandler)
logging.info('Démarrage Meross Manager')

_http_api_client, _meross_manager = asyncio.run(
    meross_connection(email=args.muser, password=args.mpswd), debug=True)

meross_root_logger = logging.getLogger("meross_iot")
meross_root_logger.setLevel(convert_log_level(args.loglevel))

logging.info('is meross_manager OK : {}'.format(_meross_manager is not None))


# Thread for JeedomHandler
t = threading.Thread(target=server.serve_forever)
t.start()
# Update Conso Power 
updateElec = UpdateAllElectricity(float(args.mupdp))
