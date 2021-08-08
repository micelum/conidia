import asyncio
import json
import logging
import os
import sys

from dotenv import load_dotenv
from nameko.standalone.rpc import ClusterRpcProxy
from aioudp import UDPServer

load_dotenv()
logging.basicConfig(stream=sys.stdout, level=os.environ.get('LOG_LEVEL'))


class Conidia:
    name = "conidia"

    def __init__(self, server):
        self.server = server
        self.server.subscribe(self.on_datagram_received)
        rpc_proxy = ClusterRpcProxy({'AMQP_URI': os.environ.get('AMQP_ADDRESS')})
        self.rpc_proxy = rpc_proxy.start()

    async def on_datagram_received(self, data, addr):
        logging.info('rcvd: message from '.format(addr))
        payload = json.loads(data.decode('utf-8'))
        logging.info('message payload decoded: {}'.format(payload))

        logging.info('Ask registry about device with mac {}'.format(payload['device']['mac']))
        registry_check_response = self.rpc_proxy.hypha_registry.check_mac(payload['device']['mac'])
        check_mac_result = json.loads(registry_check_response)
        logging.info('Registry respond {} so...'.format(check_mac_result))
        if check_mac_result['result']:  # TODO Add registration profiles
            logging.info("... we already know this device")
            response = {'status': 2}
        else:
            logging.info("... we can register this device")
            registry_register_response = self.rpc_proxy.hypha_registry.register_mac(payload['device']['mac'], payload['device']['uuid'])
            response = {'status': 1}
        septum_info = self.rpc_proxy.septum.get_connection_info()
        logging.info("Septum reports {} address".format(septum_info))
        response['septum_address'] = septum_info
        await asyncio.sleep(0.001)
        payload = json.dumps(response).encode('utf-8')
        self.server.send(payload, addr)


async def main(loop):
    udp = UDPServer()
    udp.run("0.0.0.0", 13404, loop=loop)
    server = Conidia(server=udp)


if __name__ == '__main__':
    logging.info('Application started')
    conidia_loop = asyncio.get_event_loop()
    conidia_loop.run_until_complete(main(conidia_loop))
    conidia_loop.run_forever()
