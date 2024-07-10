import time
from math import ceil
import asyncio
import dateutil.parser
from urllib.parse import unquote
from typing import Any, Tuple, Optional, Dict, List

import aiohttp
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered
from pyrogram.raw.functions.messages import RequestWebView

from bot.utils import logger
from bot.exceptions import InvalidSession
from .headers import headers
from bot.config import settings


class Miner:
    def __init__(self, tg_client: Client):
        self.session_name = tg_client.name
        self.tg_client = tg_client

        self.storage_levels = {
            '1': 2,
            '2': 4,
            '3': 6,
            '4': 12,
            '5': 24
        }

    async def get_tg_web_data(self, proxy: str | None) -> str:
        try:
            if proxy:
                proxy = Proxy.from_str(proxy)
                proxy_dict = dict(
                    scheme=proxy.protocol,
                    hostname=proxy.host,
                    port=proxy.port,
                    username=proxy.login,
                    password=proxy.password
                )
            else:
                proxy_dict = None

            self.tg_client.proxy = proxy_dict

            if not self.tg_client.is_connected:
                try:
                    await self.tg_client.connect()
                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)

            web_view = await self.tg_client.invoke(RequestWebView(
                peer=await self.tg_client.resolve_peer('Vertus_App_bot'),
                bot=await self.tg_client.resolve_peer('Vertus_App_bot'),
                platform='android',
                from_bot_menu=False,
                url='https://thevertus.app/'
            ))

            auth_url = web_view.url
            tg_web_data = unquote(
                string=unquote(
                    string=auth_url.split('tgWebAppData=', maxsplit=1)[1].split('&tgWebAppVersion', maxsplit=1)[0]))

            if self.tg_client.is_connected:
                await self.tg_client.disconnect()

            return tg_web_data

        except InvalidSession as error:
            raise error

        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error during Authorization: {error}")
            await asyncio.sleep(delay=7)

    async def get_data(self, http_client: aiohttp.ClientSession) -> Dict[str, Any]:
        url = f"{settings.API_URL}/users/get-data"
        try:
            async with http_client.request(
                    method="POST",
                    url=url,
                    data={},
            ) as response:
                response.raise_for_status()

                account = await response.json()
                return account
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while getting account data: {error}")
            await asyncio.sleep(delay=7)

    async def get_missions(self, http_client: aiohttp.ClientSession) -> str:
        url = f"{settings.API_URL}/missions/count"
        try:
            async with http_client.request(
                    method="GET",
                    url=url,
                    data={},
            ) as response:
                response.raise_for_status()

                text = await response.text()
                return text
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while getting missions: {error}")
            await asyncio.sleep(delay=7)

    async def claim(self, http_client: aiohttp.ClientSession) -> float:
        url = f"{settings.API_URL}/game-service/collect"
        try:
            async with http_client.request(
                    method="POST",
                    url=url,
                    data={},
            ) as response:
                response.raise_for_status()

                response_json = await response.json()
                new_balance = response_json['newBalance']
                return float(new_balance / 1000000000000000000)
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while claiming: {error}")
            await asyncio.sleep(delay=7)

            return -1.0

    async def daily_claim(self, http_client: aiohttp.ClientSession) -> Optional[Dict[str, Any]]:
        url = f"{settings.API_URL}/users/claim-daily"
        try:
            async with http_client.request(
                    method="POST",
                    url=url,
                    data={},
            ) as response:
                response.raise_for_status()

                response_json = await response.json()
                return response_json
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while claiming: {error}")
            await asyncio.sleep(delay=7)

            return None

    async def upgrade_speed(self, http_client: aiohttp.ClientSession) -> Optional[Dict[str, Any]]:
        url = f"{settings.API_URL}/users/upgrade"
        try:
            async with http_client.request(
                    method="POST",
                    url=url,
                    json={'upgrade': 'farm'},
            ) as response:
                response.raise_for_status()

                response_json = await response.json()
                return response_json
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while getting account data: {error}")
            await asyncio.sleep(delay=7)

            return None

    async def upgrade_storage(self, http_client: aiohttp.ClientSession) -> Optional[Dict[str, Any]]:
        url = f"{settings.API_URL}/users/upgrade"
        try:
            async with http_client.request(
                    method="POST",
                    url=url,
                    json={'upgrade': 'storage'},
            ) as response:
                response.raise_for_status()

                response_json = await response.json()
                return response_json
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while getting account data: {error}")
            await asyncio.sleep(delay=7)

            return None

    async def upgrade_population(self, http_client: aiohttp.ClientSession) -> Optional[Dict[str, Any]]:
        url = f"{settings.API_URL}/users/upgrade"
        try:
            async with http_client.request(
                    method="POST",
                    url=url,
                    json={'upgrade': 'population'},
            ) as response:
                response.raise_for_status()

                response_json = await response.json()
                return response_json
        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error while getting account data: {error}")
            await asyncio.sleep(delay=7)

            return None

    async def check_proxy(self, http_client: aiohttp.ClientSession, proxy: Proxy) -> None:
        try:
            response = await http_client.get(url='https://httpbin.org/ip', timeout=aiohttp.ClientTimeout(5))
            ip = (await response.json()).get('origin')
            logger.info(f"{self.session_name} | Proxy IP: {ip}")
        except Exception as error:
            logger.error(f"{self.session_name} | Proxy: {proxy} | Error: {error}")

    def get_current_speed(self, account: dict) -> float:
        fd = account['user']['abilities']['farm']['description']

        cindex = fd.index('coin')
        if cindex < 0:
            return -1.0

        fd = fd[:cindex].strip()
        sp = fd.index(' ')
        if sp < 0:
            return -1.0

        fd = fd[sp:].strip()
        try:
            speed = float(fd.strip())
            multiplier = float(account['user']['abilities']['population']['value'])
            return speed * multiplier
        except Exception as e:
            return -1.0

    def is_claim_possible(self, account: dict) -> Tuple[bool, int]:
        storage_volume = account['user']['vertStorage'] / 1000000000000000000
        storage_level = account['user']['abilities']['storage']['level']
        storage_max_hours = self.storage_levels.get(str(storage_level))
        speed = self.get_current_speed(account)
        max_storage_volume = storage_max_hours * speed
        percent = ceil(100 * (storage_volume / max_storage_volume))

        if percent < settings.CLAIM_MIN_PERCENT:
            time_to_claim = storage_max_hours * 3600 * (100 - percent) / 100
            return False, time_to_claim

        return True, storage_max_hours * 3600

    def is_daily_claim_possible(self, account: dict) -> bool:
        if not account:
            return False

        last_daily_claimed = account['user']['dailyRewards']['lastRewardClaimed']
        if not last_daily_claimed:
            return True

        last_claimed_timestamp = dateutil.parser.parse(last_daily_claimed).timestamp()
        current_time = time.time()

        diff = current_time - last_claimed_timestamp
        if diff > 24 * 3600:
            return True
        return False

    async def run(self, proxy: str | None) -> None:
        bearer = None
        access_token_created_time = 0
        proxy_conn = ProxyConnector().from_url(proxy) if proxy else None

        async with (aiohttp.ClientSession(headers=headers, connector=proxy_conn) as http_client):
            if proxy:
                await self.check_proxy(http_client=http_client, proxy=proxy)

            while True:
                try:
                    if not bearer or time.time() - access_token_created_time >= 3600:
                        tg_web_data = await self.get_tg_web_data(proxy=proxy)
                        bearer = tg_web_data

                        http_client.headers["Authorization"] = f"Bearer {bearer}"
                        headers["Authorization"] = f"Bearer {bearer}"

                        access_token_created_time = time.time()

                    logger.info(f"{self.session_name} | Loading account info")
                    account = await self.get_data(http_client=http_client)
                    if settings.LOAD_MISSIONS:
                        logger.info(f"{self.session_name} | Loading missions")
                        await self.get_missions(http_client=http_client)

                    balance = account['user']['balance'] / 1000000000000000000
                    storage = account['user']['vertStorage'] / 1000000000000000000

                    speed = self.get_current_speed(account)
                    logger.info(f"{self.session_name} | balance {balance:.6f} VERT, volume {storage:.6f} VERT, mining at {speed: .6f} coin / hour")

                    daily_claimable = self.is_daily_claim_possible(account=account)
                    if daily_claimable:
                        logger.info(f"{self.session_name} | Daily claim is possible, trying to claim")
                        daily_claim_info = await self.daily_claim(http_client=http_client)
                        if daily_claim_info:
                            success = daily_claim_info['success']
                            if success:
                                balance = daily_claim_info['balance'] / 1000000000000000000
                                logger.success(f"{self.session_name} | Daily claim was successful, new balance is {balance: .6f}")
                            else:
                                msg = daily_claim_info['msg']
                                logger.error(f"{self.session_name} | Daily claim was unsuccessful, error message is {msg}")

                    claimable, time_to_sleep = self.is_claim_possible(account=account)
                    sleep_time = time_to_sleep

                    if claimable:
                        retry = 0
                        while retry < settings.CLAIM_RETRY_COUNT:
                            logger.info(f"{self.session_name} | Retry <y>{retry+1}</y> of <e>{settings.CLAIM_RETRY_COUNT}</e>")
                            new_balance = await self.claim(http_client=http_client)
                            if new_balance >= 0:
                                logger.success(f'{self.session_name} | Claimed successful, new balance is {new_balance:.6f} VERT')
                                balance = new_balance
                                break

                            retry += 1

                    if settings.UPGRADE_SPEED:
                        next_level_dict = account['user']['abilities']['farm'].get('nextLevel')
                        if next_level_dict is not None:
                            if next_level_dict.get('priceToLevelUp'):
                                price = float(next_level_dict['priceToLevelUp'])
                                level = next_level_dict['level']
                                if balance >= price and settings.SPEED_MAX_LEVEL >= level:
                                    logger.info(f"{self.session_name} | Sleep 5s before upgrade speed to {level} lvl")
                                    await asyncio.sleep(delay=5)

                                    upgrade_result = await self.upgrade_speed(http_client=http_client)
                                    if upgrade_result.get('success') is True:
                                        balance = float(upgrade_result['newBalance'] / 1000000000000000000)
                                        logger.success(f"{self.session_name} | Speed upgraded to {level} lvl, balance is {balance:.6f}")

                    if settings.UPGRADE_POPULATION:
                        next_level_dict = account['user']['abilities']['population'].get('nextLevel')
                        if next_level_dict is not None:
                            if next_level_dict.get('priceToLevelUp'):
                                price = float(next_level_dict['priceToLevelUp'])
                                level = next_level_dict['level']
                                if balance >= price and settings.POPULATION_MAX_LEVEL >= level:
                                    logger.info(f"{self.session_name} | Sleep 5s before upgrade population to {level} lvl")
                                    await asyncio.sleep(delay=5)

                                    upgrade_result = await self.upgrade_population(http_client=http_client)
                                    if upgrade_result.get('success') is True:
                                        balance = float(upgrade_result['newBalance'] / 1000000000000000000)
                                        logger.success(f"{self.session_name} | Population upgraded to {level} lvl, balance is {balance:.6f}")

                    if settings.UPGRADE_STORAGE:
                        next_level_dict = account['user']['abilities']['storage'].get('nextLevel')
                        if next_level_dict is not None:
                            if next_level_dict.get('priceToLevelUp'):
                                price = float(next_level_dict['priceToLevelUp'])
                                level = next_level_dict['level']
                                if balance >= price and settings.STORAGE_MAX_LEVEL >= level:
                                    logger.info(f"{self.session_name} | Sleep 5s before upgrade storage to {level} lvl")
                                    await asyncio.sleep(delay=5)

                                    upgrade_result = await self.upgrade_storage(http_client=http_client)
                                    if upgrade_result.get('success') is True:
                                        balance = float(upgrade_result['newBalance'] / 1000000000000000000)
                                        logger.success(f"{self.session_name} | Storage upgraded to {level} lvl, balance is {balance:.6f}")

                except InvalidSession as error:
                    raise error

                except Exception as error:
                    logger.error(f"{self.session_name} | Unknown error: {error}")
                    await asyncio.sleep(delay=7)

                else:
                    logger.info(f"{self.session_name} | Sleeping for the next claim {sleep_time}s")
                    await asyncio.sleep(delay=sleep_time)


async def run_miner(tg_client: Client, proxy: str | None):
    try:
        await Miner(tg_client=tg_client).run(proxy=proxy)
    except InvalidSession:
        logger.error(f"{tg_client.name} | Invalid Session")