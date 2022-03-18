import asyncio
import logging
from datetime import datetime
from typing import Tuple, List, Optional, Iterable, Union

import async_timeout
from meross_iot.controller.mixins.consumption import ConsumptionXMixin
from meross_iot.controller.mixins.electricity import ElectricityMixin
from meross_iot.http_api import MerossHttpClient
from meross_iot.manager import MerossManager, T
from meross_iot.model.credentials import MerossCloudCreds
from meross_iot.model.enums import OnlineStatus, Namespace
from meross_iot.model.http.device import HttpDeviceInfo
from meross_iot.model.http.exception import TokenExpiredException, BadLoginException, \
    UnauthorizedException, HttpApiError

logger = logging.getLogger()
DEFAULT_USER_AGENT = "MerossJEE/1.0.0"


class MerossCoordinator:
    def __init__(self,
                 email: str,
                 password: str
                 ):
        self._email = email
        self._password = password
        self._cached_creds = None
        self._skip_cert_validation = False
        self._setup_done = False
        self._register_handler = None

        # Objects not to be initialized here
        self._client = None
        self._manager = None

    async def async_fetch_http_data(self):
        try:
            async with async_timeout.timeout(10):
                devices = await self._client.async_list_devices()
                return {device.uuid: device for device in devices}
        except (BadLoginException, TokenExpiredException, UnauthorizedException) as err:
            logger.error(f'error : {err}')
            raise err
        except HttpApiError as err:
            logger.error(f'error : {err}')
            raise err

    async def initial_setup(self, register_handler):
        if self._setup_done:
            raise ValueError("This coordinator was already set up")
        try:
            self._client, http_devices, creds_renewed = await get_or_renew_creds(
                email=self._email,
                password=self._password,
                stored_creds=self._cached_creds,
            )
        except (BadLoginException, TokenExpiredException, UnauthorizedException) as err:
            logger.error(f'error : {err}')
            raise err
        except HttpApiError as err:
            logger.error(f'error : {err}')
            raise err

        if creds_renewed:
            self._cached_creds = self._client.cloud_credentials
            # TODO: voir pour sauvegarder les creds dans jeedom
        self._manager = MerossManager(
            http_client=self._client,
            auto_reconnect=True,
            mqtt_skip_cert_validation=self._skip_cert_validation,
        )

        logger.debug(f"http_devices={http_devices}")
        logger.info("Starting meross manager")
        await self._manager.async_init()
        logger.info("Discovering Meross devices...")
        await self._manager.async_device_discovery()
        self._register_handler = register_handler
        if self._register_handler is not None:
            self._manager.register_push_notification_handler_coroutine(self._register_handler)
        self._setup_done = True

    async def close(self):
        if self._register_handler is not None:
            self._manager.unregister_push_notification_handler_coroutine(self._register_handler)
        self._manager.close()
        await self._client.async_logout()

    @property
    def manager(self) -> MerossManager:
        return self._manager

    @property
    def client(self) -> MerossHttpClient:
        return self._client

    async def update_all_elec_for_all_devices(self):
        try:
            async with async_timeout.timeout(30):
                devices = self._manager.find_devices(device_class=ElectricityMixin,
                                                     online_status=OnlineStatus.ONLINE)
                devices_update = await asyncio.gather(
                    *(get_device_electricity(device=device) for device in devices))
                return devices_update
        except Exception as err:
            logger.error(f'error : {err}')
            raise err

    def find_devices(
            self,
            device_uuids: Optional[Iterable[str]] = None,
            internal_ids: Optional[Iterable[str]] = None,
            device_type: Optional[str] = None,
            device_class: Optional[Union[type, Iterable[type]]] = None,
            device_name: Optional[str] = None,
            online_status: Optional[OnlineStatus] = None,
    ) -> List[T]:
        return self._manager.find_devices(device_uuids, internal_ids, device_type,
                                          device_class, device_name, online_status)

    async def sync_device(self, uuid):
        device_tab = self.find_devices(device_uuids=[uuid])
        if device_tab is not None:
            device = device_tab[0]
            return await get_one_device_meross(device)

    async def set_on(self, uuid, channel=0):
        device_tab = self.find_devices(device_uuids=[uuid])
        if device_tab is not None:
            device = device_tab[0]
            if device.abilities[Namespace.GARAGE_DOOR_STATE]:
                await device.async_close(channel=channel)
            else:
                await device.async_turn_on(channel=channel)
            return ''
        else:
            return 'Unknow device'

    async def set_off(self, uuid, channel=0):
        device_tab = self.find_devices(device_uuids=[uuid])
        if device_tab is not None:
            device = device_tab[0]
            if device.abilities[Namespace.GARAGE_DOOR_STATE]:
                await device.async_open(channel=int(channel))
            else:
                await device.async_turn_off(channel=int(channel))
            return
        else:
            return 'Unknow device'

    async def set_lumi(self, uuid, lumi_int):
        device_tab = self.find_devices(device_uuids=[uuid])
        if device_tab is not None:
            device = device_tab[0]
            await device.async_set_light_color(luminance=int(lumi_int))
            return
        else:
            return 'Unknow device'

    async def set_temp(self, uuid, temp_int, lumi=-1):
        device_tab = self.find_devices(device_uuids=[uuid])
        if device_tab is not None:
            device = device_tab[0]
            await device.async_set_light_color(temperature=temp_int, luminance=lumi)
            return
        else:
            return 'Unknow device'

    async def set_rgb(self, uuid, rgb_int, lumi=-1):
        device_tab = self.find_devices(device_uuids=[uuid])
        if device_tab is not None:
            device = device_tab[0]
            await device.async_set_light_color(rgb=int(rgb_int), luminance=lumi)
            return
        else:
            return 'Unknow device'

    async def set_spray(self, uuid, smode=0):
        # device_tab = self.find_devices(device_uuids=[uuid])
        # if device_tab is not None:
        #     device = device_tab[0]
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

    async def get_devices_meross(self):
        devices = self.find_devices()
        logging.debug(f"liste des devices : {devices}")
        return await asyncio.gather(
            *(get_one_device_meross(device) for device in devices))

    async def update_meross_conso(self):
        try:
            async with async_timeout.timeout(30):
                devices = self.find_devices(device_class=ConsumptionXMixin,
                                            online_status=OnlineStatus.ONLINE)
                devices_update = await asyncio.gather(
                    *(get_device_consumption(device) for device in devices))
                return devices_update
        except Exception as err:
            logger.error(f'error : {err}')
            raise err


async def get_or_renew_creds(
        email: str,
        password: str,
        stored_creds: MerossCloudCreds = None,
        http_api_url: str = "https://iot.meross.com",
        ua_header: str = DEFAULT_USER_AGENT
) -> Tuple[MerossHttpClient, List[HttpDeviceInfo], bool]:
    try:
        if stored_creds is None:
            http_client = await MerossHttpClient.async_from_user_password(
                email=email, password=password, api_base_url=http_api_url, ua_header=ua_header
            )
        else:
            http_client = MerossHttpClient(
                cloud_credentials=stored_creds, api_base_url=http_api_url, ua_header=ua_header
            )
        http_devices = await http_client.async_list_devices()
        return http_client, http_devices, False
    except TokenExpiredException as e:
        # In case the current token is expired or invalid, let's try to re-login.
        logger.exception(
            "Current token has been refused by the Meross Cloud. Trying to generate a new one with "
            "stored user credentials..."
        )

        # Build a new client with username/password rather than using stored credentials
        http_client = await MerossHttpClient.async_from_user_password(
            email=email, password=password, api_base_url=http_api_url, ua_header=ua_header
        )
        http_devices = await http_client.async_list_devices()
        return http_client, http_devices, True


async def get_device_electricity(device: ElectricityMixin):
    try:
        logger.debug(f'[get_device_electricity] device : {device}')
        electricity = await device.async_get_instant_metrics()
        logger.debug(f'[get_device_electricity] electricity : {electricity}')
        d = dict({'uuid': device.uuid, 'power': 0, 'current': 0, 'voltage': 0})
        d['power'] = electricity.power
        d['voltage'] = electricity.voltage
        d['current'] = electricity.current
        return d
    except Exception as e:
        logger.error(f'[get_device_electricity] error : {e}')
        return False


async def get_device_consumption(device: ConsumptionXMixin):
    try:
        logger.debug(f'[get_device_consumption] device : {device}')
        conso = await device.async_get_daily_power_consumption()
        logger.debug(f'[get_device_consumption] conso : {conso}')
        d = dict({'uuid': device.uuid, 'conso_totale': 0})
        today = datetime.today().strftime("%Y-%m-%d")
        for c in conso:
            dateconso = c['date'].strftime("%Y-%m-%d")
            if dateconso == today:
                d['conso_totale'] = c['value']
        return d
    except Exception as e:
        logger.error(f'[get_device_consumption] error : {e}')
        return False


async def get_one_device_meross(device):
    try:
        await device.async_update()
    except BaseException as ex:
        logger.error("Erreur lors de l'async : {}".format(ex))
        pass
    logger.info("[get_one_device_meross] device : {}".format(device))
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
    logger.info(f"[get_one_device_meross] Data : {data}")
    d['values'] = {}
    # Nom Canaux
    onoff = [device.name]
    for x in device._channels:
        try:
            onoff.append(x['name'])
        except:
            pass
    d['onoff'] = onoff

    logger.info(f"[get_one_device_meross] d après onoff: {d}")
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
    # Puissance
    if Namespace.CONTROL_ELECTRICITY.value in data.keys():
        d['elec'] = True
        electricity = await device.async_get_instant_metrics()
        d['values']['power'] = electricity.power
        d['values']['current'] = electricity.current
        d['values']['voltage'] = electricity.voltage
    else:
        d['elec'] = False
    # Consommation
    if Namespace.CONTROL_CONSUMPTIONX.value in data.keys() or Namespace.CONTROL_CONSUMPTION.value in data.keys():
        d['conso'] = True
        l_conso = await device.async_get_daily_power_consumption()
        d['values']['conso_totale'] = 0
        today = datetime.today().strftime("%Y-%m-%d")
        for c in l_conso:
            dateconso = c['date'].strftime("%Y-%m-%d")
            if dateconso == today:
                logger.debug(f"[get_one_device_meross] c: {c}")
                d['values']['conso_totale'] = c['value']
    else:
        d['conso'] = False
    # Lumiere
    if Namespace.CONTROL_LIGHT.value in data.keys():
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
    if Namespace.CONTROL_SPRAY.value in data.keys():
        d['spray'] = True
        # d[device.uuid]['values']['spray'] = device.get_spray_mode().value
    else:
        d['spray'] = False
    # Fini
    return d
