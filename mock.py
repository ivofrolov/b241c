#!/usr/bin/env python3

import argparse
import itertools
import logging
import os
import pickle
import random
import sys
import threading
import time
import uuid
from base64 import b64encode
from collections import defaultdict
from decimal import Decimal
from functools import partial
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from operator import itemgetter
from pathlib import Path
from urllib.parse import parse_qsl, urlparse

import xmlschema

EMPTY = """<?xml version="1.0" encoding="UTF-8"?>
<КоммерческаяИнформация
    xmlns="urn:1C.ru:commerceml_3"
    xmlns:xs="http://www.w3.org/2001/XMLSchema"
    xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
    ВерсияСхемы="3.1"
    ДатаФормирования="2026-08-22T21:14:29">
</КоммерческаяИнформация>
"""

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


def dpath(data, path, default_factory=lambda: None):
    components = path.split(".")
    for i in range(len(components) - 1):
        if isinstance(data, list):
            data = next(iter(data), {})
        data = data.get(components[i]) or {}
    return data.get(components[-1], default_factory())


class ExchangeHandler(BaseHTTPRequestHandler):
    MAX_UPLOAD_CHUNK = 1024**2
    COOKIE_NAME = "PHPSESSID"

    auth_token = b64encode(b"user:password").decode()
    sessions = {"207d5cfb792f0cbdeae6896bde279cc9": time.time()}

    def __init__(self, *args, schema, storage, **kwargs):
        self.schema = schema
        self.storage = storage
        super().__init__(*args, **kwargs)

    def _validate_xml(self, content):
        try:
            self.schema.validate(content)
        except Exception as exc:
            logging.error(exc)
            return False
        return True

    def _store_data(self, content):
        def storeby(data, path, key):
            section = path.split(".")[-2]
            for item in dpath(data, path, list):
                self.storage[section][key(item)] = item

        data = self.schema.to_dict(content, validation="skip")
        storeby(data, "Каталог.Товары.Товар", itemgetter("Ид"))
        for path in (
            "ТипыЦен.ТипЦен",
            "Склады.Склад",
            "ЕдиницыИзмерения.ЕдиницаИзмерения",
            "Группы.Группа",
        ):
            storeby(data, f"Классификатор.{path}", itemgetter("Ид"))
        for item in dpath(data, "ПакетПредложений.Предложения.Предложение", list):
            storeby(item, "Цены.Цена", lambda p: (item["Ид"], p["ИдТипаЦены"]))
            storeby(item, "Остатки.Остаток", lambda p: (item["Ид"], p["Склад"]["Ид"]))

    def _generate_order(self):
        if not self.storage["Товары"]:
            return ORDER

        item = random.choice(list(self.storage["Товары"].values()))

        warehouse_id = "00000000-0000-0000-0000-000000000000"
        for (item_id, _), stock in self.storage["Остатки"].items():
            if item_id == item["Ид"] and stock["Склад"]["Количество"] > 0:
                warehouse_id = stock["Склад"]["Ид"]

        price = Decimal("9335.42")
        for (item_id, _), pricelist in self.storage["Цены"].items():
            if item_id == item["Ид"]:
                price = pricelist["ЦенаЗаЕдиницу"]

        items = [
            {
                "Ид": "ORDER_DELIVERY",
                "Наименование": "Доставка",
                "БазоваяЕдиница": "796",
                "Количество": Decimal("1.00"),
                "ЦенаЗаЕдиницу": Decimal("348.00"),
                "Сумма": Decimal("348.00"),
            },
            {
                "Ид": item["Ид"],
                "Наименование": item["Наименование"],
                "БазоваяЕдиница": "796",
                "ЗначенияРеквизитов": {
                    "ЗначениеРеквизита": [
                        {"Наименование": "Склад", "Значение": [warehouse_id]},
                    ]
                },
                "Количество": Decimal("1.00"),
                "ЦенаЗаЕдиницу": price,
                "Сумма": price,
            },
        ]
        partner = {
            "Ид": ["da51150b-9955-4734-bf3a-ae2c3c54181a"],
            "Наименование": ["Иванов Иван Иванович"],
            "Контакты": [
                {"Контакт": [{"Тип": "Телефон рабочий", "Значение": "+01234567890"}]},
            ],
        }
        total = sum([item["Сумма"] for item in items])
        order = {
            "Ид": str(uuid.uuid4()),
            "Номер": str(random.randint(1000, 10000)),
            "Дата": time.strftime("%Y-%m-%d"),
            "Время": time.strftime("%H:%M:%S"),
            "ХозОперация": "Заказ товара",
            "Контрагенты": {"Контрагент": [partner]},
            "Валюта": "RUB",
            "Курс": "1",
            "Сумма": total,
            "Основание": None,
            "Роль": "Продавец",
            "Товары": {"Товар": items},
        }
        payment = {
            "Ид": str(uuid.uuid4()),
            "Номер": str(random.randint(1000, 10000)),
            "Дата": time.strftime("%Y-%m-%d"),
            "Время": time.strftime("%H:%M:%S"),
            "ХозОперация": "Эквайринговая операция",
            "Контрагенты": {"Контрагент": [partner]},
            "Валюта": "RUB",
            "Курс": "1",
            "Сумма": total,
            "Основание": order["Ид"],
            "Роль": "Продавец",
        }
        docs = {
            "@xmlns": "urn:1C.ru:commerceml_3",
            "@xmlns:xs": "http://www.w3.org/2001/XMLSchema",
            "@xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
            "@ВерсияСхемы": "3.1",
            "@ДатаФормирования": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "Контейнер": [{"Документ": [order, payment]}],
        }

        content = xmlschema.etree_tostring(
            self.schema.encode(docs, "КоммерческаяИнформация"),
            namespaces={
                "": "urn:1C.ru:commerceml_3",
                "xs": "http://www.w3.org/2001/XMLSchema",
                "xsi": "http://www.w3.org/2001/XMLSchema-instance",
            },
            encoding="utf-8",
            xml_declaration=True,
        )
        return content.decode("utf-8")

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
        logging.info("%s:\n%s", query.get("filename", "<file>"), body)
        if self._validate_xml(body):
            self._store_data(body)
            body = "success"
        else:
            body = "failure"
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
        body = EMPTY
        if self.storage.get("_send_order", True):
            body = self._generate_order()
        if not self._validate_xml(body):
            self.send_error(HTTPStatus.INTERNAL_SERVER_ERROR)
            return
        logging.info("order.xml:\n%s", body)
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

        schema_path = Path(args.schema).resolve()
        self.schema = xmlschema.XMLSchema(schema_path)
        self.watched[schema_path] = 0

        self.cache_path = Path(args.cache).resolve() if args.cache else None
        self.storage = defaultdict(dict)

    def load_storage(self):
        if self.cache_path and self.cache_path.exists():
            with self.cache_path.open("rb") as fp:
                self.storage.update(pickle.load(fp))

    def dump_storage(self):
        if self.cache_path:
            with self.cache_path.open("wb") as fp:
                pickle.dump(self.storage, fp)

    def run(self):
        self.load_storage()
        self.start_server()
        for path in self.watched:
            self.watched[path] = path.stat().st_mtime

        while not self.timer.wait(self.POLL_INTERVAL):
            self.poll()

    def cancel(self):
        self.timer.set()
        self.stop_server()
        self.dump_storage()

    def poll(self):
        for path, last_mtime in self.watched.items():
            mtime = path.stat().st_mtime
            if mtime > last_mtime:
                break
        else:
            return

        logging.warning("reloading")
        self.cancel()
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
            partial(ExchangeHandler, schema=self.schema, storage=self.storage),
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
    parser.add_argument("--cache", help="persistent cache location")
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
