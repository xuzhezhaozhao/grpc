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

import contextlib
import logging
import grpc
from grpc import _grpcio_metadata

from tests.unit import test_common
from tests.unit.framework.common import test_constants
from tests.unit import _tcp_proxy

_UNARY_UNARY = '/test/UnaryUnary'
_STREAM_STREAM = '/test/StreamStream'


def handle_unary_uncompressed(request, servicer_context):
    return request


def handle_unary_compressed(request, servicer_context):
    servicer_context.send_initial_metadata([('grpc-internal-encoding-request',
                                             'gzip')])
    return request


def handle_stream_uncompressed(request_iterator, servicer_context):
    for request in request_iterator:
        yield request


def handle_stream_compressed(request_iterator, servicer_context):
    # TODO(issue:#6891) We should be able to remove this loop,
    # and replace with return; yield
    servicer_context.send_initial_metadata([('grpc-internal-encoding-request',
                                             'gzip')])
    for request in request_iterator:
        yield request


class _MethodHandler(grpc.RpcMethodHandler):

    def __init__(self, request_streaming, response_streaming, compressed):
        self.request_streaming = request_streaming
        self.response_streaming = response_streaming
        self.request_deserializer = None
        self.response_serializer = None
        self.unary_unary = None
        self.unary_stream = None
        self.stream_unary = None
        self.stream_stream = None
        if self.request_streaming and self.response_streaming:
            if compressed:
                self.stream_stream = handle_stream_compressed
            else:
                self.stream_stream = handle_stream_uncompressed
        elif not self.request_streaming and not self.response_streaming:
            if compressed:
                self.unary_unary = handle_unary_compressed
            else:
                self.unary_unary = handle_unary_uncompressed


class _GenericHandler(grpc.GenericRpcHandler):

    def __init__(self, compressed=True):
        self._compressed = compressed

    def service(self, handler_call_details):
        if handler_call_details.method == _UNARY_UNARY:
            return _MethodHandler(False, False, self._compressed)
        elif handler_call_details.method == _STREAM_STREAM:
            return _MethodHandler(True, True, self._compressed)
        else:
            return None


@contextlib.contextmanager
def _instrumented_client_server_pair(channel_kwargs, server_handler):
    server = test_common.test_server()
    server.add_generic_rpc_handlers((server_handler,))
    server_port = server.add_insecure_port('[::]:0')
    server.start()
    with _tcp_proxy.TcpProxy('localhost', 'localhost', server_port) as proxy:
        proxy_port = proxy.get_port()
        with grpc.insecure_channel('localhost:{}'.format(proxy_port),
                                   **channel_kwargs) as client_channel:
            try:
                yield client_channel, proxy, server
            finally:
                server.stop(None)


def _get_byte_counts(channel_kwargs, server_handler, message):
    with _instrumented_client_server_pair(channel_kwargs,
                                          server_handler) as pipeline:
        client_channel, proxy, server = pipeline
        multi_callable = client_channel.unary_unary(_UNARY_UNARY)
        response = multi_callable(message)
        if response != message:
            raise RuntimeError("Request '{}' != Response '{}'".format(
                message, response))
        return proxy.get_byte_count()


def _get_byte_differences(first_channel_kwargs, first_server_handler,
                          second_channel_kwargs, second_server_handler,
                          message):
    first_bytes_sent, first_bytes_received = _get_byte_counts(
        first_channel_kwargs, first_server_handler, message)
    second_bytes_sent, second_bytes_received = _get_byte_counts(
        second_channel_kwargs, second_server_handler, message)
    return second_bytes_sent - first_bytes_sent, second_bytes_received - first_bytes_received


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
        uncompressed_channel_kwargs = {}
        compressed_channel_kwargs = {
            'compression': grpc.CompressionAlgorithm.deflate,
        }
        bytes_sent_difference, bytes_received_difference = _get_byte_differences(
            uncompressed_channel_kwargs, _GenericHandler(False),
            compressed_channel_kwargs, _GenericHandler(True), request)
        print("Bytes sent difference: {}".format(bytes_sent_difference))
        print("Bytes received difference: {}".format(bytes_received_difference))
        self.assertLess(bytes_sent_difference, 0)
        self.assertLess(bytes_received_difference, 0)

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
