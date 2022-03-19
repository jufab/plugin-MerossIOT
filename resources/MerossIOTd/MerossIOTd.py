import argparse
import asyncio
import json
import logging
import os
import signal
import sys
from asyncio import AbstractEventLoop
from concurrent.futures import ThreadPoolExecutor

from jeedom import JeedomCallback
from meross import MerossCoordinator


async def update_device(meross_coordinator: MerossCoordinator, jc: JeedomCallback, delay: int):
    while True:
        await asyncio.sleep(delay=delay)
        devices = await meross_coordinator.update_all_elec_for_all_devices()
        logging.debug(f'devices : {devices}')
        if devices is not {}:
            logging.info('Send Electricity')
            jc.send({'action': 'electricity', 'values': devices})
        else:
            logging.debug('No Send')


async def handler_jeedom(reader, writer):
    data = await reader.read(1024)
    message = json.loads(data.decode())
    lmessage = dict(message)
    del lmessage['apikey']
    logging.info(f"Received : {lmessage}")
    if message.get('apikey') != _api_key:
        logging.error(f"Invalid apikey from socket : {data}")
        writer.write("error".encode())
        await writer.drain()
        writer.close()
    response = {'result': None, 'success': True}
    action = message.get('action')
    args = message.get('args')
    if hasattr(meross_coordinator, action):
        func = getattr(meross_coordinator, action)
        response['result'] = func
        logging.debug(f"response avant appel : {response}")
        if callable(response['result']):
            response['result'] = await response['result'](*args)
    logging.info(f"Response socket : {response}")
    writer.write(json.dumps(response).encode())
    await writer.drain()
    logging.debug('Close the client socket')
    writer.close()


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
    logging.info("Arrêt")
    for task in asyncio.all_tasks(main_loop):
        task.cancel()
    asyncio.ensure_future(meross_coordinator.close())
    main_loop.stop()
    jc.stop()
    logging.debug("Effacement fichier PID " + str(_pidfile))
    if os.path.exists(_pidfile):
        os.remove(_pidfile)
    logging.debug("Effacement fichier socket " + str(_sockfile))
    if os.path.exists(_sockfile):
        os.remove(_sockfile)


def main() -> None:
    asyncio.set_event_loop(main_loop)
    executor = ThreadPoolExecutor(max_workers=3, )
    main_loop.set_default_executor(executor)
    asyncio.ensure_future(meross_coordinator.initial_setup(jc.event_handler))
    asyncio.ensure_future(asyncio.start_unix_server(handler_jeedom, path=_sockfile))
    asyncio.ensure_future(update_device(meross_coordinator=meross_coordinator, jc=jc, delay=_delay))
    main_loop.run_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--muser', help='Compte Meross', default='')
    parser.add_argument('--mpswd', help='Mot de passe Meross', default='')
    parser.add_argument('--mupdp', help='Fréquence actualisation puissance', type=int, default=30)
    parser.add_argument('--callback', help='Jeedom callback', default='http://localhost')
    parser.add_argument('--apikey', help='API Key', default='nokey')
    parser.add_argument('--loglevel', help='LOG Level', default='error')
    parser.add_argument('--pidfile', help='PID File', default='MerossIOTd.pid')
    parser.add_argument('--socket', help='Daemon socket', default='MerossIOTd.sock')
    args = parser.parse_args()

    _delay = args.mupdp
    _pidfile = args.pidfile
    _sockfile = args.socket
    _api_key = args.apikey

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)

    pid = str(os.getpid())
    logging.debug("Ecriture du PID " + pid + " dans " + str(args.pidfile))
    with open(args.pidfile, 'w') as fp:
        fp.write("%s\n" % pid)

    if os.path.exists(_sockfile):
        os.unlink(_sockfile)

    FORMAT = '[%(asctime)-15s][%(levelname)s][%(name)s](%(threadName)s) : %(message)s'
    logging.basicConfig(level=convert_log_level(args.loglevel), format=FORMAT,
                        datefmt="%Y-%m-%d %H:%M:%S")
    logging.getLogger().setLevel(convert_log_level(args.loglevel))
    meross_root_logger = logging.getLogger("meross_iot")
    meross_root_logger.setLevel(convert_log_level(args.loglevel))

    jc = JeedomCallback(_api_key, args.callback)
    if not jc.test():
        sys.exit()

    meross_coordinator: MerossCoordinator = MerossCoordinator(email=args.muser, password=args.mpswd)
    main_loop: AbstractEventLoop = asyncio.new_event_loop()
    main()
