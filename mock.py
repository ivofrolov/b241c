#!/usr/bin/env python3

import argparse
import itertools
import logging
import os
import sys
import threading
import time
from base64 import b64encode
from functools import partial
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qsl, urlparse


ORDER = """<?xml version="1.0" encoding="UTF-8"?>
<КоммерческаяИнформация
    xmlns="urn:1C.ru:commerceml_3"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    ВерсияСхемы="3.1"
    ДатаФормирования="2026-08-22T21:14:29">
  <Контейнер>
    <Документ>
      <Ид>bce8cd28-6206-481a-ab23-f614b3a3e0a8</Ид>
      <Номер>1</Номер>
      <Дата>2026-08-22</Дата>
      <Время>21:14:29</Время>
      <ХозОперация>Заказ товара</ХозОперация>
      <Контрагенты>
        <Контрагент>
          <Ид>da51150b-9955-4734-bf3a-ae2c3c54181a</Ид>
          <Наименование>Иванов Иван Иванович</Наименование>
          <Контакты>
            <Контакт>
              <Тип>Телефон рабочий</Тип>
              <Значение>+01234567890</Значение>
            </Контакт>
          </Контакты>
        </Контрагент>
      </Контрагенты>
      <Валюта>RUB</Валюта>
      <Курс>1</Курс>
      <Сумма>9683.42</Сумма>
      <Основание></Основание>
      <Роль>Продавец</Роль>
      <Товары>
        <Товар>
          <Ид>ORDER_DELIVERY</Ид>
          <Наименование>Доставка</Наименование>
          <БазоваяЕдиница>796 </БазоваяЕдиница>
          <Количество>1.00</Количество>
          <ЦенаЗаЕдиницу>348.00</ЦенаЗаЕдиницу>
          <Сумма>348.00</Сумма>
        </Товар>
        <Товар>
          <Ид>dee6e1aa-55bc-11d9-848a-00112f43529a</Ид>
          <Наименование>Пылесос "Омега" 1250вт</Наименование>
          <БазоваяЕдиница>796 </БазоваяЕдиница>
          <ЗначенияРеквизитов>
            <ЗначениеРеквизита>
              <Наименование>Склад</Наименование>
              <Значение>b63f7fc6-4347-11ee-841a-e16e604b2373</Значение>
            </ЗначениеРеквизита>
          </ЗначенияРеквизитов>
          <Количество>1.00</Количество>
          <ЦенаЗаЕдиницу>9335.42</ЦенаЗаЕдиницу>
          <Сумма>9335.42</Сумма>
        </Товар>
      </Товары>
    </Документ>
    <Документ>
      <Ид>52cbe99a-7cd5-46ae-acfa-048b258f23c2</Ид>
      <Номер>1</Номер>
      <Дата>2026-08-22</Дата>
      <Время>21:14:29</Время>
      <ХозОперация>Эквайринговая операция</ХозОперация>
      <Контрагенты>
        <Контрагент>
          <Ид>da51150b-9955-4734-bf3a-ae2c3c54181a</Ид>
          <Наименование>Иванов Иван Иванович</Наименование>
          <Контакты>
            <Контакт>
              <Тип>Телефон рабочий</Тип>
              <Значение>+01234567890</Значение>
            </Контакт>
          </Контакты>
        </Контрагент>
      </Контрагенты>
      <Валюта>RUB</Валюта>
      <Курс>1</Курс>
      <Сумма>9683.42</Сумма>
      <Основание></Основание>
      <Роль>Продавец</Роль>
    </Документ>
  </Контейнер>
</КоммерческаяИнформация>
"""


class ExchangeHandler(BaseHTTPRequestHandler):
    MAX_UPLOAD_CHUNK = 1024**2
    COOKIE_NAME = "PHPSESSID"

    auth_token = b64encode(b"user:password").decode()
    sessions = {"207d5cfb792f0cbdeae6896bde279cc9": time.time()}

    def __init__(self, *args, schema=None, **kwargs):
        self.schema = schema
        super().__init__(*args, **kwargs)

    def _validate_xml(self, content):
        if self.schema is None:
            return True
        try:
            self.schema.validate(content)
        except Exception as exc:
            logging.error(exc)
            return False
        return True

    def _authenticate(self):
        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            return False
        _, token = header.split(" ", maxsplit=1)
        return token == self.auth_token

    def _validate_session(self):
        value = None
        for chunk in self.headers.get("Cookie", "").split(";"):
            name, sep, val = chunk.strip().partition("=")
            if sep and name == self.COOKIE_NAME:
                value = val
                break
        return bool(value) and value in self.sessions

    def _read_body(self):
        try:
            length = int(self.headers.get("Content-Length") or 0)
        except ValueError:
            length = 0
        remaining = length
        while remaining > 0:
            chunk = self.rfile.read(min(self.MAX_UPLOAD_CHUNK, remaining))
            if not chunk:
                break
            yield chunk.decode()
            remaining -= len(chunk)

    def _respond(self, code, *, body=None, headers=None):
        self.send_response(code)
        if headers:
            for name, value in headers.items():
                self.send_header(name, value)
        if body:
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path
        query = dict(parse_qsl(parsed.query))

        if path != "/1c_exchange.php":
            self.send_error(HTTPStatus.NOT_FOUND)
            return

        if not self._authenticate():
            self.send_error(HTTPStatus.UNAUTHORIZED)
            return

        if mode := query.get("mode"):
            if handler := getattr(self, f"_handle_{mode}", None):
                if mode != "checkauth":
                    if not self._validate_session():
                        self.send_error(HTTPStatus.UNAUTHORIZED)
                        return
                handler(query)
                return

        self.send_error(HTTPStatus.BAD_REQUEST)

    def _handle_checkauth(self, query):
        # value = secrets.token_hex(16)
        # self.sessions[value] = time.time()
        value = next(iter(self.sessions))

        body = f"success\n{self.COOKIE_NAME}\n{value}"
        self._respond(
            HTTPStatus.OK,
            body=body.encode(),
            headers={"Set-Cookie": f"{self.COOKIE_NAME}={value}; Path=/; HttpOnly"},
        )

    def _handle_init(self, query):
        body = f"zip=no\nfile_limit={self.MAX_UPLOAD_CHUNK}"
        if query["type"] == "sale":
            body += "\n\nversion 3.1"
        self._respond(HTTPStatus.OK, body=body.encode())

    def _handle_file(self, query):
        body = "".join(self._read_body())
        logging.info("%s:\n%s", query.get("filename", "<file>"), "".join(body))
        body = "success" if self._validate_xml(body) else "failure"
        self._respond(HTTPStatus.OK, body=body.encode())

    def _handle_import(self, query):
        body = "success"
        self._respond(HTTPStatus.OK, body=body.encode())

    def _handle_complete(self, query):
        body = "success"
        self._respond(HTTPStatus.OK, body=body.encode())

    def _handle_deactivate(self, query):
        body = "success"
        self._respond(HTTPStatus.OK, body=body.encode())

    def _handle_query(self, query):
        body = ORDER
        if not self._validate_xml(body):
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        self._respond(
            HTTPStatus.OK,
            body=body.encode(),
            headers={"Content-Type": "application/xml"},
        )

    def _handle_success(self, query):
        body = "success"
        self._respond(HTTPStatus.OK, body=body.encode())


class HotReloadServer:
    POLL_INTERVAL = 1.0

    def __init__(self, args):
        self.args = args

        self.watched = {Path(__file__).resolve(): 0}
        self.timer = threading.Event()
        self.server = None

        self.schema = None
        try:
            import xmlschema

            schema_path = Path(args.schema).resolve()
            self.schema = xmlschema.XMLSchema(schema_path)
            self.watched[schema_path] = 0
        except ImportError:
            logging.warning("validation disabled: xmlschema not installed")

    def run(self):
        self.start_server()
        for path in self.watched:
            self.watched[path] = path.stat().st_mtime

        while not self.timer.wait(self.POLL_INTERVAL):
            self.poll()

    def cancel(self):
        self.timer.set()
        self.stop_server()

    def poll(self):
        for path, last_mtime in self.watched.items():
            mtime = path.stat().st_mtime
            if mtime > last_mtime:
                break
        else:
            return

        logging.warning("reloading")
        self.stop_server()
        args = ["python3", __file__]
        args.extend(
            itertools.chain.from_iterable(
                (f"--{name}", str(value)) for name, value in vars(self.args).items()
            )
        )
        os.execv(sys.executable, args)

    def stop_server(self):
        if self.server is not None:
            self.server.shutdown()
            self.server.server_close()

    def start_server(self):
        self.server = ThreadingHTTPServer(
            (self.args.host, self.args.port),
            partial(ExchangeHandler, schema=self.schema),
        )
        logging.info("listening on http://%s:%s", self.args.host, self.args.port)
        logging.info(
            'use basic auth header: "Authorization: Basic %s"',
            ExchangeHandler.auth_token,
        )
        self.thread = threading.Thread(target=self.server.serve_forever)
        self.thread.start()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="1C website exchange mock server")
    parser.add_argument("--host", default="localhost", help="bind address")
    parser.add_argument("--port", type=int, default=8000, help="bind port")
    parser.add_argument("--schema", default="СхемаОбмена.xsd", help="XML schema")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(message)s",
    )

    server = HotReloadServer(args)
    try:
        server.run()
    except KeyboardInterrupt:
        logging.info("shutting down")
    finally:
        server.cancel()
