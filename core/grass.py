import asyncio
import random
import uuid
from typing import List, Optional
import csv
import aiohttp
from fake_useragent import UserAgent
from tenacity import stop_after_attempt, retry, retry_if_not_exception_type, wait_random, retry_if_exception_type

from data.config import MIN_PROXY_SCORE, CHECK_POINTS, STOP_ACCOUNTS_WHEN_SITE_IS_DOWN

try:
    from data.config import SHOW_LOGS_RARELY
except ImportError:
    SHOW_LOGS_RARELY = ""

from .grass_sdk.extension import GrassWs
from .grass_sdk.website import GrassRest
from .utils import logger

from .utils.accounts_db import AccountsDB
from .utils.error_helper import raise_error, FailureCounter
from .utils.exception import WebsocketClosedException, LowProxyScoreException, ProxyScoreNotFoundException, \
    ProxyForbiddenException, ProxyError, WebsocketConnectionFailedError, FailureLimitReachedException, \
    NoProxiesException, ProxyBlockedException, SiteIsDownException, LoginException
from better_proxy import Proxy
import datetime
import ssl
import json
import time
from websockets_proxy import Proxy as wss_proxy
from websockets_proxy import proxy_connect

class Grass(GrassWs, GrassRest, FailureCounter):
    # global_fail_counter = 0

    def __init__(self, _id: int, email: str, password: str, proxy: str = None, wss_proxy: str = None, db: AccountsDB = None):
        self.proxy = Proxy.from_str(proxy).as_url if proxy else None
        self.wss_proxy = wss_proxy
        super(GrassWs, self).__init__(email=email, password=password, user_agent=UserAgent().random, proxy=self.proxy)
        self.proxy_score: Optional[int] = None
        self.id: int = _id

        self.db: AccountsDB = db

        self.session: aiohttp.ClientSession = aiohttp.ClientSession(trust_env=True,
                                                                    connector=aiohttp.TCPConnector(ssl=False))

        self.proxies: List[str] = []
        self.is_extra_proxies_left: bool = True

        self.fail_count = 0
        self.limit = 5

    async def start(self):
        self.proxies = await self.db.get_proxies_by_email(self.email)
        self.log_global_count(True)
        # logger.info(f"{self.id} | {self.email} | Starting...")
        while True:
            try:
                Grass.is_site_down()

                user_id = await self.enter_account()

                browser_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, self.proxy or ""))

                await self.run(browser_id, user_id)
            except LoginException as e:
                logger.warning(f"LoginException | {self.id} | {e}")
                return False
            except (ProxyBlockedException, ProxyForbiddenException) as e:
                # self.proxies.remove(self.proxy)
                msg = "Proxy forbidden"
            except ProxyError:
                msg = "Low proxy score"
            except WebsocketConnectionFailedError:
                msg = "Websocket connection failed"
                self.reach_fail_limit()
            except aiohttp.ClientError as e:
                msg = f"{str(e.args[0])[:30]}..." if "</html>" not in str(e) else "Html page response, 504"
            except FailureLimitReachedException as e:
                msg = "Failure limit reached"
                self.reach_fail_limit()
            except SiteIsDownException as e:
                msg = f"Site is down!"
                self.reach_fail_limit()
            else:
                msg = ""

            await self.failure_handler(
                is_raise=False,
            )

            await self.change_proxy()
            logger.info(f"{self.id} | Changed proxy to {self.proxy}. {msg}. Retrying...")

            await asyncio.sleep(random.uniform(20, 30))

    async def connect_to_wss(self, socks5_proxy, user_id):
        device_id = str(uuid.uuid3(uuid.NAMESPACE_DNS, socks5_proxy))
        logger.info(f"Connection started for proxy {socks5_proxy} device_id:{device_id}")
        while True:
            try:
                # 连接成功
                await asyncio.sleep(random.randint(1, 10) / 10)
                custom_headers = {
                    "User-Agent": self.user_agent
                }
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                uri = "wss://proxy.wynd.network:4650/"
                server_hostname = "proxy.wynd.network:4650"
                proxy = wss_proxy.from_url(socks5_proxy)

                async with proxy_connect(uri, proxy=proxy, ssl=ssl_context, server_hostname=server_hostname,
                                         extra_headers=custom_headers) as websocket:
                    async def send_ping():
                        while True:
                            send_message = json.dumps(
                                {"id": str(uuid.uuid4()), "version": "1.0.0", "action": "PING", "data": {}})
                            logger.debug(send_message)
                            await websocket.send(send_message)
                            await asyncio.sleep(20)

                    # asyncio.create_task(send_http_request_every_10_seconds(socks5_proxy, device_id))
                    await asyncio.sleep(1)
                    asyncio.create_task(send_ping())

                    while True:
                        response = await websocket.recv()
                        message = json.loads(response)
                        logger.debug(message)
                        # update_connection_stats(socks5_proxy, connected=True)

                        if message.get("action") == "AUTH":
                            auth_response = {
                                "id": message["id"],
                                "origin_action": "AUTH",
                                "result": {
                                    "browser_id": device_id,
                                    "user_id": user_id,
                                    "user_agent": custom_headers['User-Agent'],
                                    "timestamp": int(time.time()),
                                    "device_type": "extension",
                                    "version": "4.0.1"
                                }
                            }
                            logger.debug(auth_response)
                            await websocket.send(json.dumps(auth_response))

                        elif message.get("action") == "PONG":
                            pong_response = {"id": message["id"], "origin_action": "PONG"}
                            logger.debug(pong_response)
                            await websocket.send(json.dumps(pong_response))


            except Exception as e:
                logger.error(f'socks5_proxy:{socks5_proxy} error:{e}')
                await asyncio.sleep(1, 60)

    async def run(self, browser_id: str, user_id: str):
        while True:
            try:
                # await self.connection_handler()
                # await self.auth_to_extension(browser_id, user_id)
                await self.connect_to_wss(self.wss_proxy, user_id)

                # if self.proxy_score is None:
                #     await asyncio.sleep(1)
                #
                #     await self.handle_proxy_score(MIN_PROXY_SCORE)
                #
                # for i in range(999999999):
                #     await self.send_ping()
                #     await self.send_pong()
                #
                #     if SHOW_LOGS_RARELY:
                #         if not (i % 10):
                #             logger.info(f"{self.id} | Mined grass.")
                #     else:
                #         logger.info(f"{self.id} | Mined grass.")
                #
                #     if CHECK_POINTS and not (i % 100):
                #         points = await self.get_points_handler()
                #         logger.info(f"{self.id} | Total points: {points}")
                #
                #     if i:
                #         self.fail_reset()
                #
                #     await asyncio.sleep(19.9)
            except WebsocketClosedException as e:
                logger.info(f"{self.id} | Websocket closed: {e}. Reconnecting...")
            except ConnectionResetError as e:
                logger.info(f"{self.id} | Connection reset: {e}. Reconnecting...")
            except TypeError as e:
                logger.info(f"{self.id} | Type error: {e}. Reconnecting...")
                await self.delay_with_log(msg=f"{self.id} | Reconnecting with delay for some minutes...", sleep_time=60)

            await self.failure_handler(limit=4)

            await asyncio.sleep(5, 10)

    async def claim_rewards(self):
        await self.enter_account()
        await self.claim_rewards_handler()

        logger.info(f"{self.id} | Claimed all rewards.")

    # async def check_point(self):
    #     userId = await self.enter_account()
    #     points = await self.get_points_handler()
    #     logger.info(f"{self.id} | Total points: {points}")
    #
    #     with open(f"logs/accounts_point_{datetime.date.today().strftime('%Y-%m-%d')}.csv", "a", encoding="utf-8") as f:
    #         f.write(f"{self.email}:{self.password}:{self.username},{userId},{points}\n")
    #     logger.info(f"{self.id} | Check all rewards.")

    async def check_ip_normal(self, data):
        results = []
        for item in data['result']['data']:
            status = "normal" if item['ipScore'] >= 75 else "error"
            results.append({
                'ipAddress': item['ipAddress'],
                'ipScore': item['ipScore'],
                'status': status
            })
        return results

    async def check_point(self):
        userId = await self.enter_account()
        points = await self.get_points_handler()
        logger.info(f"{self.id} | Total points: {points}")

        data = await self.get_all_devices_info()
        ip_result = await self.check_ip_normal(data)

        file_path = f"logs/accounts_point_{datetime.date.today().strftime('%Y-%m-%d')}.csv"
        with open(file_path, "a", newline='', encoding="utf-8") as file:
            csv_writer = csv.writer(file)
            csv_writer.writerow([self.email, self.password, self.username, userId, points, ip_result])

        logger.info(f"{self.id} | Check all rewards.")

    @retry(stop=stop_after_attempt(12),
           retry=(retry_if_exception_type(ConnectionError) | retry_if_not_exception_type(ProxyForbiddenException)),
           retry_error_callback=lambda retry_state:
           raise_error(WebsocketConnectionFailedError(f"{retry_state.outcome.exception()}")),
           wait=wait_random(7, 10),
           reraise=True)
    async def connection_handler(self):
        logger.info(f"{self.id} | Connecting...")
        await self.connect()
        logger.info(f"{self.id} | Connected")

    @retry(stop=stop_after_attempt(5),
           retry=retry_if_not_exception_type(LowProxyScoreException),
           before_sleep=lambda retry_state, **kwargs: logger.info(f"{retry_state.outcome.exception()}"),
           wait=wait_random(5, 7),
           reraise=True)
    async def handle_proxy_score(self, min_score: int):
        if (proxy_score := await self.get_proxy_score_by_device_id_handler()) is None:
            # logger.info(f"{self.id} | Proxy score not found for {self.proxy}. Guess Bad proxies! Continue...")
            # return None
            raise ProxyScoreNotFoundException(f"{self.id} | Proxy score not found! Retrying...")
        elif proxy_score >= min_score:
            self.proxy_score = proxy_score
            logger.success(f"{self.id} | Proxy score: {self.proxy_score}")
            return True
        else:
            raise LowProxyScoreException(f"{self.id} | Too low proxy score: {proxy_score} for {self.proxy}. Retrying...")

    async def change_proxy(self):
        self.proxy = await self.get_new_proxy()

    async def get_new_proxy(self):
        while self.is_extra_proxies_left:
            if (proxy := await self.db.get_new_from_extra_proxies("ProxyList")) is not None:
                if proxy not in self.proxies:
                    if email := await self.db.proxies_exist(proxy):
                        if self.email == email:
                            self.proxies.insert(0, proxy)
                            break
                    else:
                        await self.db.add_account(self.email, proxy)
                        self.proxies.insert(0, proxy)
                        break
            else:
                self.is_extra_proxies_left = False

        return await self.next_proxy()

    async def next_proxy(self):
        if not self.proxies:
            await self.reset_with_delay(f"{self.id} | No proxies left. Use same proxy...", 30 * 60)
            return self.proxy
            # raise NoProxiesException(f"{self.id} | No proxies left. Exiting...")

        proxy = self.proxies.pop(0)
        self.proxies.append(proxy)

        return proxy

    @staticmethod
    def is_site_down():
        if STOP_ACCOUNTS_WHEN_SITE_IS_DOWN and Grass.is_global_error():
            logger.info(f"Site is down. Sleeping for non-working accounts...")
            raise SiteIsDownException()
