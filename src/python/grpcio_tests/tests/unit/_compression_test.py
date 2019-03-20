# Copyright 2016 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests server and client side compression."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import unittest

import datetime
import logging
import select
import socket
import threading
import grpc
from grpc import _grpcio_metadata

from tests.unit import test_common
from tests.unit.framework.common import test_constants

_UNARY_UNARY = '/test/UnaryUnary'
_STREAM_STREAM = '/test/StreamStream'


def handle_unary(request, servicer_context):
    servicer_context.send_initial_metadata([('grpc-internal-encoding-request',
                                             'gzip')])
    return request


def handle_stream(request_iterator, servicer_context):
    # TODO(issue:#6891) We should be able to remove this loop,
    # and replace with return; yield
    servicer_context.send_initial_metadata([('grpc-internal-encoding-request',
                                             'gzip')])
    for request in request_iterator:
        yield request


class _MethodHandler(grpc.RpcMethodHandler):

    def __init__(self, request_streaming, response_streaming):
        self.request_streaming = request_streaming
        self.response_streaming = response_streaming
        self.request_deserializer = None
        self.response_serializer = None
        self.unary_unary = None
        self.unary_stream = None
        self.stream_unary = None
        self.stream_stream = None
        if self.request_streaming and self.response_streaming:
            self.stream_stream = handle_stream
        elif not self.request_streaming and not self.response_streaming:
            self.unary_unary = handle_unary


class _GenericHandler(grpc.GenericRpcHandler):

    def service(self, handler_call_details):
        if handler_call_details.method == _UNARY_UNARY:
            return _MethodHandler(False, False)
        elif handler_call_details.method == _STREAM_STREAM:
            return _MethodHandler(True, True)
        else:
            return None

# TODO(rbellevi): Pull out into its own module.
_TCP_PROXY_BUFFER_SIZE = 1024
_TCP_PROXY_TIMEOUT = datetime.timedelta(milliseconds=500)


def _init_listen_socket(bind_address):
    listen_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listen_socket.bind((bind_address, 0))
    listen_socket.listen(1)
    return listen_socket, listen_socket.getsockname()[1]


def _init_proxy_socket(gateway_address, gateway_port):
    proxy_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proxy_socket.connect((gateway_address, gateway_port))
    return proxy_socket


class TcpProxy(object):
    def __init__(self, bind_address, gateway_address, gateway_port):
        self._bind_address = bind_address
        self._gateway_address = gateway_address
        self._gateway_port = gateway_port

        self._byte_count_lock = threading.RLock()
        self._sent_byte_count = 0
        self._received_byte_count = 0

        self._stop_event = threading.Event()

        self._listen_socket, self._port = _init_listen_socket(self._bind_address)
        self._proxy_socket = _init_proxy_socket(self._gateway_address, self._gateway_port)

        self._thread = threading.Thread(target=self._run_proxy)
        self._thread.start()

    def get_port(self):
        return self._port

    # TODO: Modularize this method
    def _run_proxy(self):
        client_sockets = []
        northbound_data = ""
        southbound_data = ""
        while not self._stop_event.is_set():
            expected_reads = (self._listen_socket, self._proxy_socket) + tuple(client_sockets)
            expected_writes = expected_reads
            sockets_to_read, sockets_to_write, _ = select.select(expected_reads, expected_writes, (), _TCP_PROXY_TIMEOUT.total_seconds())
            for socket_to_read in sockets_to_read:
                if socket_to_read is self._listen_socket:
                    client_socket, client_address = socket_to_read.accept()
                    client_sockets.append(client_socket)
                elif socket_to_read is self._proxy_socket:
                    data = socket_to_read.recv(_TCP_PROXY_BUFFER_SIZE)
                    with self._byte_count_lock:
                        self._received_byte_count += len(data)
                    northbound_data += data
                else:
                    data = socket_to_read.recv(_TCP_PROXY_BUFFER_SIZE)
                    if data:
                        with self._byte_count_lock:
                            self._sent_byte_count += len(data)
                        southbound_data += data
                    else:
                        client_sockets.remove(socket_to_read)
                for socket_to_write in sockets_to_write:
                    if socket_to_write is self._proxy_socket:
                        if southbound_data:
                            self._proxy_socket.sendall(southbound_data)
                            southbound_data = ""
                    else:
                        if northbound_data:
                            socket_to_write.sendall(northbound_data)
                            northbound_data = ""
        for client_socket in client_sockets:
            client_socket.close()

    def stop(self):
        self._stop_event.set()
        self._thread.join()
        self._listen_socket.close()
        self._proxy_socket.close()

    def get_byte_count(self):
        with self._byte_count_lock:
            return self._sent_byte_count, self._received_byte_count,

    def reset_byte_count(self):
        with self._byte_count_lock:
            self._byte_count = 0
            self._received_byte_count = 0

    def __enter__(self):
        return self._port

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


class CompressionTest(unittest.TestCase):

    def setUp(self):
        pass
        # self._server = test_common.test_server()
        # self._server.add_generic_rpc_handlers((_GenericHandler(),))
        # self._port = self._server.add_insecure_port('[::]:0')
        # self._server.start()

    def tearDown(self):
        pass
        # self._server.stop(None)

    def testUnary(self):
        request = b'\x00' * 100
        server = test_common.test_server()
        server.add_generic_rpc_handlers((_GenericHandler(),))
        server_port = server.add_insecure_port('[::]:0')
        server.start()
        proxy = TcpProxy('localhost', 'localhost', server_port)
        proxy_port = proxy.get_port()
        client_channel = grpc.insecure_channel('localhost:{}'.format(proxy_port))
        multi_callable = client_channel.unary_unary(_UNARY_UNARY)
        response = multi_callable(request)
        self.assertEqual(request, response)
        bytes_sent, bytes_received = proxy.get_byte_count()
        print("Bytes sent: {}".format(bytes_sent))
        print("Bytes received: {}".format(bytes_received))
        self.assertGreater(bytes_sent, 0)
        self.assertGreater(bytes_received, 0)
        server.stop(None)
        proxy.stop()
        client_channel.close()

    # def testUnary(self):
    #     request = b'\x00' * 100

    #     # Client -> server compressed through default client channel compression
    #     # settings. Server -> client compressed via server-side metadata setting.
    #     # TODO(https://github.com/grpc/grpc/issues/4078): replace the "1" integer
    #     # literal with proper use of the public API.
    #     compressed_channel = grpc.insecure_channel(
    #         'localhost:%d' % self._port,
    #         options=[('grpc.default_compression_algorithm', 1)])
    #     multi_callable = compressed_channel.unary_unary(_UNARY_UNARY)
    #     response = multi_callable(request)
    #     self.assertEqual(request, response)

    #     # Client -> server compressed through client metadata setting. Server ->
    #     # client compressed via server-side metadata setting.
    #     # TODO(https://github.com/grpc/grpc/issues/4078): replace the "0" integer
    #     # literal with proper use of the public API.
    #     uncompressed_channel = grpc.insecure_channel(
    #         'localhost:%d' % self._port,
    #         options=[('grpc.default_compression_algorithm', 0)])
    #     multi_callable = compressed_channel.unary_unary(_UNARY_UNARY)
    #     response = multi_callable(
    #         request, metadata=[('grpc-internal-encoding-request', 'gzip')])
    #     self.assertEqual(request, response)
    #     compressed_channel.close()

    # def testStreaming(self):
    #     request = b'\x00' * 100

    #     # TODO(https://github.com/grpc/grpc/issues/4078): replace the "1" integer
    #     # literal with proper use of the public API.
    #     compressed_channel = grpc.insecure_channel(
    #         'localhost:%d' % self._port,
    #         options=[('grpc.default_compression_algorithm', 1)])
    #     multi_callable = compressed_channel.stream_stream(_STREAM_STREAM)
    #     call = multi_callable(iter([request] * test_constants.STREAM_LENGTH))
    #     for response in call:
    #         self.assertEqual(request, response)
    #     compressed_channel.close()


if __name__ == '__main__':
    logging.basicConfig()
    unittest.main(verbosity=2)
