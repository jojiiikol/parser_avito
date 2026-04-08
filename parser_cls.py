import asyncio
import html
import json
import random
import re
import time
import traceback
import typing
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import dateparser
import playwright
from bs4 import BeautifulSoup
from loguru import logger
from playwright.async_api import async_playwright
from playwright.sync_api import Playwright, sync_playwright
from pydantic import ValidationError

from common_data import HEADERS
from db_service import SQLiteDBHandler
from dto import Proxy, AvitoConfig
from filters.ads_filter import AdsFilter
from hide_private_data import log_config
from integrations.notifications.factory import build_notifier
from load_config import load_avito_config
from models import ItemsResponse, Item, Review, AnswerOnReview, Seller
from parser.cookies.factory import build_cookies_provider
from parser.export.factory import build_result_storage
from parser.http.aioclient import AioHttpClient
from parser.http.client import HttpClient
from parser.proxies.proxy_factory import build_proxy
from utils.parse_phone import ParsePhone
from utils.remove_emojies import remove_emojis
from version import VERSION

DEBUG_MODE = False

logger.add("logs/app.log", rotation="5 MB", retention="5 days", level="DEBUG")

instance_count = 1
URLS = {
    # "https://www.avito.ru/all/predlozheniya_uslug/obuchenie_kursy/predmeti_shkoli_i_vuza-ASgBAgICAkSYC7afAaQrkrgC?cd=1": 100,
    # "https://www.avito.ru/all/predlozheniya_uslug/obuchenie_kursy/inostrannie_yaziki-ASgBAgICAkSYC7afAaQrjrgC?cd=1": 100,
    # "https://www.avito.ru/all/predlozheniya_uslug/obuchenie_kursy/detskoe_razvitie_logopedi-ASgBAgICAkSYC7afAaQrjLgC?cd=1": 100,
    # "https://www.avito.ru/all/predlozheniya_uslug/obuchenie_kursy/professii_i_biznes-ASgBAgICAkSYC7afAaQrsPCNAw?cd=1": 100,
    # "https://www.avito.ru/all/predlozheniya_uslug/obuchenie_kursy/tvorchestvo_hobbi_sport-ASgBAgICAkSYC7afAaQrlrgC?cd=1": 100,
    # "https://www.avito.ru/all/predlozheniya_uslug/obuchenie_kursy/vozhdenie-ASgBAgICAkSYC7afAaQrirgC?cd=1": 100,
    # "https://www.avito.ru/all/predlozheniya_uslug/obuchenie_kursy/pomoshch_v_oformlenii_rabot-ASgBAgICAkSYC7afAaQrlLgC?cd=1": 100,
    # "https://www.avito.ru/all/predlozheniya_uslug/obuchenie_kursy/duhovnye_praktiki-ASgBAgICAkSYC7afAaQrmrgC?cd=1": 100,
    # "https://www.avito.ru/all/predlozheniya_uslug/obuchenie_kursy/drugoe-ASgBAgICAkSYC7afAaQrkLgC?cd=1": 100,

    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/manikyur_pedikyur-ASgBAgICAkSYC6qfAaIrgLgC?716=10197": 100,
    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/uslugi_parikmahera-ASgBAgICAkSYC6qfAaIrhrgC?716=10197": 100,
    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/brovi_resnicy-ASgBAgICAkSYC6qfAaIrrOSKAw?716=10197": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/permanentnyy_makiyazh-ASgBAgICAkSYC6qfAaIr9JSQAw?716=10197": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/kosmetologiya-ASgBAgICAkSYC6qfAaIrkvCNAw?716=10197": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/epilyaciya-ASgBAgICAkSYC6qfAaIrlPCNAw?716=10197": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/makiyazh-ASgBAgICAkSYC6qfAaIr_rcC?716=10197": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/spa_uslugi_massazh-ASgBAgICAkSYC6qfAaIrgrgC?716=10197": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/tatu_pirsing-ASgBAgICAkSYC6qfAaIrhLgC?716=10197": 20,
    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/arenda_rabochego_mesta-ASgBAgICAkSYC6qfAaIrmPCNAw?716=10197": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/krasota/drugoe-ASgBAgICAkSYC6qfAaIriLgC?716=10197": 50,
    #
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/remont_kvartir_i_domov_pod_klyuch-ASgBAgICAkSYC8CfAcQVwPUB?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/dizajn_intererov-ASgBAgICAkSYC8CfAcQVsOmOAw?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/santekhnika-ASgBAgICAkSYC8CfAcQVsvUB?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/elektrika-ASgBAgICAkSYC8CfAcQVsPUB?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/sborka_i_remont_mebeli-ASgBAgICAkSYC8CfAcQVrPUB?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/ostekleniye_balkonov-ASgBAgICAkSYC8CfAcQVtvUB?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/master_na_chas-ASgBAgICAkSYC8CfAcQVloyaAw?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/vskrytie_i_remont_zamkov-ASgBAgICAkSYC8CfAcQVmIyaAw?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/poklejka_oboev_i_malyarnye_raboty-ASgBAgICAkSYC8CfAcQVyvr9Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/potolki-ASgBAgICAkSYC8CfAcQVzvr9Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/poly_i_napolnye_pokrytiya-ASgBAgICAkSYC8CfAcQVzPr9Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/shtukaturnye_raboty-ASgBAgICAkSYC8CfAcQVgv39Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/dveri-ASgBAgICAkSYC8CfAcQVwPr9Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/plitochnye_raboty-ASgBAgICAkSYC8CfAcQVyPr9Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/stolyarnye_i_plotnitskie_raboty-ASgBAgICAkSYC8CfAcQVgP39Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/gipsokartonnye_raboty-ASgBAgICAkSYC8CfAcQVvvr9Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/vysotnye_raboty-ASgBAgICAkSYC8CfAcQVvPr9Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/izolyatsiya-ASgBAgICAkSYC8CfAcQVwvr9Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/remont_kommercheskih_pomeshcheniy-ASgBAgICAkSYC8CfAcQVtPUB?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/ventilyatsiya-ASgBAgICAkSYC8CfAcQVuvr9Ag?716=10208": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_otdelka/drugoe-ASgBAgICAkSYC8CfAcQV0Pr9Ag?716=10208": 50,
    #
    # "https://www.avito.ru/all/predlozheniya_uslug/zdorove/psihologiya-ASgBAgICAkSYC_CylQPahhb0spUD?cd=1": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/zdorove/dietologiya-ASgBAgICAkSYC_CylQPahhbyspUD?cd=1": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/zdorove/fitnes_joga-ASgBAgICAkSYC_CylQPahhb2spUD?cd=1": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/zdorove/stomatologiya-ASgBAgICAkSYC_CylQPahhba3pYD?cd=1": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/zdorovye/podologiya-ASgBAgICAkSYC_CylQPahhbc3pYD?cd=1": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/zdorove/drugoe-ASgBAgICAkSYC_CylQPahhbe3pYD?cd=1": 4,
    #
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/stroitelstvo_domov_pod_klyuch-ASgBAgICAkSYC6Cf8QLejw_~nYsD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/stroitelstvo_garazhej_bani_verand-ASgBAgICAkSYC6Cf8QLejw~cn_EC?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/otdelka_derevyannyh_domov-ASgBAgICAkSYC6Cf8QLejw_ckIcD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/kladochnye_raboty-ASgBAgICAkSYC6Cf8QLejw_snYsD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/krovelnye_raboty-ASgBAgICAkSYC6Cf8QLejw_0nYsD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/svarka_kovka_metallokonstrukcii-ASgBAgICAkSYC6Cf8QLejw_6nYsD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/fundamentnye_i_betonnye_raboty-ASgBAgICAkSYC6Cf8QLejw~CnosD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/almaznoe_sverlenie_i_rezka-ASgBAgICAkSYC6Cf8QLejw_unYsD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/snos_i_demontazh-ASgBAgICAkSYC6Cf8QLejw_8nYsD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/fasadnye_raboty-ASgBAgICAkSYC6Cf8QLejw~AnosD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/proektirovanie_i_smety-ASgBAgICAkSYC6Cf8QLejw_4nYsD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/raznorabochie-ASgBAgICAkSYC6Cf8QLejw~MvY8D?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/izyskatelnye_raboty-ASgBAgICAkSYC6Cf8QLejw_qnYsD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/lestnicy-ASgBAgICAkSYC6Cf8QLejw_2nYsD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/gazifikaciya-ASgBAgICAkSYC6Cf8QLejw_ynYsD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/kommercheskoe_stroitelstvo-ASgBAgICAkSYC6Cf8QLejw~AkZoD?716=3024848": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/stroitelstvo/drugoe-ASgBAgICAkSYC6Cf8QLejw~EnosD?716=3024848": 50,
    #
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/televizor-ASgBAgICAkSYC7T3Ad4V2vcB?716=15834": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/mobilnyye_ustroystva-ASgBAgICAkSYC7T3Ad4V4vcB?716=15834": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/foto_audio_videotekhnika-ASgBAgICAkSYC7T3Ad4V5vcB?716=15834": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/kondicionery_i_ventilyaciya-ASgBAgICAkSYC7T3Ad4V0qCOAw?716=15834": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/stiralnye_syshilnye_mashiny-ASgBAgICAkSYC7T3Ad4V1KCOAw?716=15834": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/posudomoechnye_mashiny-ASgBAgICAkSYC7T3Ad4V1qCOAw?716=15834": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/holodilniki_i_morozilnye_kamery-ASgBAgICAkSYC7T3Ad4V2KCOAw?716=15834": 50,
    # "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/kuhonnye_plity_varochnye_paneli-ASgBAgICAkSYC7T3Ad4V2qCOAw?716=15834": 50,
    "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/gazovye_kotly_vodonagrevateli-ASgBAgICAkSYC7T3Ad4V3KCOAw?716=15834": 100,
    "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/kofemashiny-ASgBAgICAkSYC7T3Ad4V3qCOAw?716=15834": 100,
    "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/shvejnyemashiny_overloki-ASgBAgICAkSYC7T3Ad4V4KCOAw?716=15834": 100,
    "https://www.avito.ru/all/predlozheniya_uslug/remont_i_obsluzhivanie_tehniki/drugoe-ASgBAgICAkSYC7T3Ad4V7KCOAw?716=15834": 100,

    "https://www.avito.ru/all/predlozheniya_uslug/bytovye_uslugi/izgotovlenie_klyuchej_i_zatochka-ASgBAgICAkSYC7CfAZwL4p8B?cd=1": 100,
    "https://www.avito.ru/all/predlozheniya_uslug/bytovye_uslugi/poshiv_i_remont_odezhdy-ASgBAgICAkSYC7CfAZwL5p8B?cd=1": 100,
    "https://www.avito.ru/all/predlozheniya_uslug/bytovye_uslugi/remont_chasov-ASgBAgICAkSYC7CfAZwL6p8B?cd=1": 100,
    "https://www.avito.ru/all/predlozheniya_uslug/bytovye_uslugi/himchistka_stirka-ASgBAgICAkSYC7CfAZwL8p8B?cd=1": 100,
    "https://www.avito.ru/all/predlozheniya_uslug/bytovye_uslugi/yuvelirnye_uslugi-ASgBAgICAkSYC7CfAZwL9J8B?cd=1": 100
}


class AvitoParse:
    def __init__(
            self,
            config: AvitoConfig,
            stop_event=None
    ):
        self.config = config
        self.proxy = build_proxy(self.config)
        self.cookies_provider = build_cookies_provider(config=config)
        self.db_handler = SQLiteDBHandler()
        self.notifier = build_notifier(config=config)
        self.result_storage = None
        self.stop_event = stop_event
        self.headers = HEADERS
        self.parse_count = 0
        self.good_request_count = 0
        self.bad_request_count = 0
        self.http = AioHttpClient(
            proxy=self.proxy,
            cookies=self.cookies_provider,
            timeout=20,
            max_retries=self.config.max_count_of_retry,
        )
        self.ads_filter = AdsFilter(config=config, is_viewed_fn=self.is_viewed)
        log_config(config=self.config, version=VERSION)

    def get_proxy_obj(self) -> Proxy | None:
        if all([self.config.proxy_string, self.config.proxy_change_url]):
            return Proxy(
                proxy_string=self.config.proxy_string,
                change_ip_link=self.config.proxy_change_url
            )
        logger.info("Работаем без прокси")
        return None

    async def fetch_data(self, url: str) -> str | None:
        if self.stop_event and self.stop_event.is_set():
            return None

        try:
            response = await self.http.request("GET", url)
            self.good_request_count += 1
            return response.text

        except Exception as err:
            self.bad_request_count += 1
            logger.warning(f"Ошибка при запросе {url}: {err}")
            return None

    async def fetch_seller_url_and_json(self, ad_json: dict) -> tuple[str, dict] | None:
        profile_info = ad_json.get("contactBarInfo", {}).get("publicProfileInfo", {})

        is_shop = profile_info.get("shopInfo").get("isShop", None)
        if is_shop is not None:
            if is_shop:
                seller_url = profile_info.get("shopInfo").get("shopLink")
            else:
                seller_url = profile_info.get("publicProfile").get("link")
            seller_page = await self.fetch_data(url=f"https://www.avito.ru{seller_url}")
            seller_json = self.find_json_on_seller_page(seller_page)
            return seller_url, seller_json
        return None, None

    async def parse(self):
        if not self.config.one_file_for_link:
            # один storage на весь парсинг
            self.result_storage = build_result_storage(config=self.config)

        for _index, url in enumerate(list(URLS.keys())):
            logger.info(f"Начинаю парсить категорию {url}")

            if self.config.one_file_for_link:
                # storage для этой ссылки
                self.result_storage = build_result_storage(
                    config=self.config,
                    link_index=_index
                )

            first_page = await self.fetch_data(url=url)
            html_page_count = self._extract_page_count(html_code=first_page)
            logger.info(f"Найдено {html_page_count} страниц")

            page_count = URLS[url] * instance_count
            if page_count > html_page_count:
                page_count = html_page_count
            logger.info(f"Используется количество в {page_count} страниц")
            for i in range(0, page_count):
                ads_in_link = []
                logger.info(f"page={i + 1}")
                if i != 0:
                    if self.stop_event and self.stop_event.is_set():
                        return
                    if DEBUG_MODE:
                        html_code = open("may.txt", "r", encoding="utf-8").read()
                    else:
                        html_code = await self.fetch_data(url=url)
                else:
                    html_code = first_page

                if not html_code:
                    logger.warning(
                        f"Не удалось получить HTML для {url}, пробую заново через {self.config.pause_between_links} сек.")
                    time.sleep(self.config.pause_between_links)
                    continue

                data_from_page = self.find_json_on_page(html_code=html_code)

                try:
                    catalog = data_from_page.get("catalog") or {}
                    ads_models = ItemsResponse(**catalog)
                except ValidationError as err:
                    logger.error(f"При валидации объявлений произошла ошибка: {err}")
                    continue

                ads = self._clean_null_ads(ads=ads_models.items)

                logger.info(f"Объявлений перед чисткой {len(ads)}")

                ads = self._add_seller_to_ads(ads=ads)

                if not ads:
                    logger.info("Объявления закончились, заканчиваю работу с данной ссылкой")
                    break

                filter_ads = self.filter_ads(ads=ads)

                self.notifier.notify_many(ads=filter_ads)

                # Глубокий парсинг
                filter_ads = await self.deep_parse(ads=filter_ads)

                # Телефоны
                filter_ads = self.parse_phone(ads=filter_ads)

                if filter_ads:
                    self.__save_viewed(ads=filter_ads)
                    ads_in_link.extend(filter_ads)

                url = self.get_next_page_url(url=url)

                logger.info(f"Пауза {self.config.pause_between_links} сек.")
                time.sleep(self.config.pause_between_links)

                if ads_in_link:
                    logger.info(f"Сохраняю {len(ads_in_link)} объявлений")
                    self.result_storage.save(ads_in_link)
                else:
                    logger.info("Сохранять нечего")

        logger.info(f"Хорошие запросы: {self.good_request_count}шт, плохие: {self.bad_request_count}шт")

        if self.config.one_time_start:
            self.notifier.notify(message="Парсинг Авито завершён. Все ссылки обработаны")
            self.stop_event = True

    def clean_html(self, raw_html: str) -> str:
        return (re.sub(r"<.*?>", "", raw_html)
                .replace("\\/", "/")
                .replace("\xa0", " ")
                .replace("  ", " ")
                .replace("\n", " ")
                .replace("\t", " ")
                .replace("\r", " "))

    @staticmethod
    def _clean_null_ads(ads: list[Item]) -> list[Item]:
        return [ad for ad in ads if ad.id]

    @staticmethod
    def find_json_on_page(html_code, data_type: str = "mime") -> dict:
        import html as html_lib
        html_code = BeautifulSoup(html_code, "html.parser")
        try:
            for _script in html_code.select('script'):

                script_type = _script.get('type')

                if data_type == 'mime':
                    for script in html_code.select('script'):
                        if script.get('type') == 'mime/invalid' and script.get(
                                'data-mfe-state') == 'true' and 'sandbox' not in script.text:
                            data = json.loads(html_lib.unescape(script.text))
                            if data.get('i18n', {}).get('hasMessages', {}):
                                return data.get('state', {}).get('data', {})
        except Exception as err:
            logger.error(f"Ошибка при поиске информации на странице: {err}")
        return {}

    @staticmethod
    def find_json_on_seller_page(html_code, data_type: str = "mime") -> dict:
        import html as html_lib
        html_code = BeautifulSoup(html_code, "html.parser")
        try:
            for _script in html_code.select('script'):

                script_type = _script.get('type')

                if data_type == 'mime':
                    for script in html_code.select('script'):
                        if script.get('type') == 'mime/invalid' and script.get(
                                'data-mfe-state') == 'true' and 'sandbox' not in script.text:
                            data = json.loads(html_lib.unescape(script.text))
                            if data.get('i18n', {}).get('hasMessages', {}):
                                return data.get('state', {})
        except Exception as err:
            logger.error(f"Ошибка при поиске информации на странице: {err}")
        return {}

    def find_json_on_ad_page(self, html_code: str) -> dict:
        html_code = BeautifulSoup(html_code, "html.parser")
        try:
            for _script in html_code.select('script'):
                if not "loaderData" in _script.text:
                    continue

                json_text = _script.text
                json_text = json_text.replace("window.__staticRouterHydrationData = JSON.parse(", "")
                json_text = json_text[:len(json_text) - 2]

                json_text = json.loads(json_text)
                json_text = json.loads(json_text)

                main_data = json_text.get("loaderData", {}).get("catalog-or-main-or-item", {}).get("buyerItem", {})
                return main_data

            return {}
        except Exception as err:
            logger.error(f"Ошибка при получении json со страницы объявления")
        return {}

    def filter_ads(self, ads: list[Item]) -> list[Item]:
        return self.ads_filter.apply(ads)

    def _add_seller_to_ads(self, ads: list[Item]) -> list[Item]:
        for ad in ads:
            if seller_id := self._extract_seller_slug(data=ad):
                ad.sellerId = seller_id
        return ads

    @staticmethod
    def _add_promotion_to_ads(ads: list[Item]) -> list[Item]:
        for ad in ads:
            ad.isPromotion = any(
                v.get("title") == "Продвинуто"
                for step in (ad.iva or {}).get("DateInfoStep", [])
                for v in step.payload.get("vas", [])
            )
        return ads

    async def deep_parse(self, ads: list[Item]) -> list[Item]:
        if not self.config.parse_views:
            return ads

        async def parallel(ad):
            try:
                logger.info("Засыпаю...")
                await asyncio.sleep(20 + random.randint(5, 10))
                html_code_full_page = await self.fetch_data(url=f"https://www.avito.ru{ad.urlPath}")
                # html_code_full_page = await self.fetch_data(url=f"https://www.avito.ru//volgograd/predlozheniya_uslug/tatu_tatu-master_2377521191?context=H4sIAAAAAAAA_wE_AMD_YToyOntzOjEzOiJsb2NhbFByaW9yaXR5IjtiOjA7czoxOiJ4IjtzOjE2OiJUcGNwN0xYVVlFOVR3ZXR2Ijt90kVpBD8AAAA")
                if not html_code_full_page:
                    return
                await asyncio.sleep(10 + random.randint(5, 10))
                ad_json = self.find_json_on_ad_page(html_code_full_page)
                seller_url, seller_json = await self.fetch_seller_url_and_json(ad_json=ad_json)
                if seller_url:
                    ad.seller = await self._extract_seller_info(ad_json=ad_json, seller_json=seller_json)
                    ad.reviews, ad.count_reviews = await self._extract_reviews(ad=ad, seller_json=seller_json)
                    ad.seller.url = seller_url
                ad.total_views, ad.today_views = self._extract_views(html=html_code_full_page)
                ad.videos = self._extract_videos(ad_json=ad_json)
                ad.description = self._extract_full_description(html_code=html_code_full_page, ad=ad)
                ad.score = self._extract_score(ad_json=ad_json)
                ad.category.specification = self._extract_specification(ad_json=ad_json)
                ad.additional_info = self._extract_additional_info(ad_json=ad_json)
                ad.price_list = self._extract_price_list(html_code=html_code_full_page)

                delay = random.uniform(1, 2)
                await asyncio.sleep(delay)
                logger.info("Обработано")
                return ad
            except Exception as err:
                logger.error(f"Ошибка при парсинге {ad.urlPath}: {traceback.print_exc()}", exc_info=True)
                return

        tasks = [parallel(ad) for ad in ads]
        await asyncio.gather(*tasks)

        # for ad_index, ad in enumerate(ads):
        #     try:
        #         html_code_full_page = await self.fetch_data(url=f"https://www.avito.ru{ad.urlPath}")
        #         # html_code_full_page = await self.fetch_data(url=f"https://www.avito.ru//volgograd/predlozheniya_uslug/tatu_tatu-master_2377521191?context=H4sIAAAAAAAA_wE_AMD_YToyOntzOjEzOiJsb2NhbFByaW9yaXR5IjtiOjA7czoxOiJ4IjtzOjE2OiJUcGNwN0xYVVlFOVR3ZXR2Ijt90kVpBD8AAAA")
        #         if not html_code_full_page:
        #             continue
        #         delay = random.uniform(1, 2)
        #         time.sleep(delay)
        #         ad_json = self.find_json_on_ad_page(html_code_full_page)
        #         seller_url, seller_json = await self.fetch_seller_url_and_json(ad_json=ad_json)
        #         if seller_url:
        #             ad.seller = await self._extract_seller_info(ad_json=ad_json, seller_json=seller_json)
        #             ad.reviews, ad.count_reviews = await self._extract_reviews(ad=ad, seller_json=seller_json)
        #             ad.seller.url = seller_url
        #         ad.total_views, ad.today_views = self._extract_views(html=html_code_full_page)
        #         ad.videos = self._extract_videos(ad_json=ad_json)
        #         ad.description = self._extract_full_description(html_code=html_code_full_page, ad=ad)
        #         ad.score = self._extract_score(ad_json=ad_json)
        #         ad.category.specification = self._extract_specification(ad_json=ad_json)
        #         ad.additional_info = self._extract_additional_info(ad_json=ad_json)
        #         ad.price_list = self._extract_price_list(html_code=html_code_full_page)
        #
        #         delay = random.uniform(1, 2)
        #         time.sleep(delay)
        #         self.parse_count += 1
        #         logger.info(f"Обработано {self.parse_count} объявлений")
        #     except Exception as err:
        #         logger.error(f"Ошибка при парсинге {ad.urlPath}: {err}", exc_info=True)
        #         continue
        return ads

    def parse_phone(self, ads: list[Item]) -> list[Item]:
        if not self.config.parse_phone or self.config.parse_phone:
            # future feat
            return ads

        try:
            return ParsePhone(ads=ads, config=self.config).parse_phones()
        except Exception as err:
            logger.warning(f"Ошибка при парсинге телефонов: {err}")
            return ads

    def _extract_page_count(self, html_code: str) -> int:
        try:
            soup = BeautifulSoup(html_code, "html.parser")
            tag = "a", {"data-marker": re.compile("pagination-button/page.+")}
            buttons = soup.find_all(*tag)
            last_button = buttons[-1]
            value = last_button.get("data-value", "50")
            return int(value)
        except Exception as err:
            logger.warning(f"Ошибка при парсинге количества страниц: {err}")
            return 50

    def _extract_full_description(self, html_code: str, ad: Item) -> str | None:
        try:
            soup = BeautifulSoup(html_code, "html.parser")
            tag = "div", {"data-marker": "item-view/item-description"}

            description = soup.find(*tag)
            cleaned_text = self.clean_html(remove_emojis(description.text.strip()))
            return cleaned_text
        except Exception as err:
            logger.warning(f"Ошибка при получении полного описания: {err}")
            return ad.description

    def _extract_additional_info(self, ad_json: dict) -> str | None:
        try:
            if additionals := ad_json.get("paramsBlock", {}).get("items", []):
                if additionals[-1].get("title") == "Опыт работы":
                    additionals = additionals[:len(additionals) - 1]

                additionals_text = [f"{add.get('title')} - {add.get('description')}" for add in additionals]
                return "\n".join(additionals_text)
            return None
        except Exception as err:
            logger.warning("Ошибка при парсинге дополнительной информации")
            return None

    def _extract_price_list(self, html_code: str) -> str | None:
        try:
            soup = BeautifulSoup(html_code, "html.parser")
            tag = "div", {"data-marker": re.compile("PRICE_LIST_VALUE_MARKER.+")}

            if prices := soup.find_all(*tag):
                prices_text = [self.clean_html(price.get_text(" - ", strip=True)) for price in prices]
                return "\n".join(prices_text)
            return None
        except Exception as err:
            logger.warning(f"Ошибка при парсинге прайс листа: {err}")
            return None

    def _extract_videos(self, ad_json: dict) -> list[str]:
        try:
            urls = []
            media = ad_json.get("galleryInfo", {}).get("media", [])
            filtered_media = list(filter(lambda item: item.get("isVideo") == True, media))
            if filtered_media:
                urls = [list(item.get("urls", {}).values())[0] for item in filtered_media]
            return urls
        except Exception as err:
            logger.warning(f"При парсинге видео прозошла ошибка: {err}")
            return []

    def _extract_experience(self, ad_json: dict) -> str | None:
        try:
            experience = ad_json.get("paramsDto", {}).get("items", [])[-1]
            if experience and experience.get("title") == "Опыт работы":
                experience = experience.get("description", None)
                return experience
            return None
        except Exception as err:
            logger.warning(f"Ошибка при парсинге опыта работы: {err}")
            return None

    def _extract_characteristics(self, ad_json: dict) -> str:
        try:
            if badge_bar := ad_json.get("item", {}).get("sellerBadgeBar", {}):
                badges = badge_bar.get("badges", [])
                badges_list = [self.clean_html(badge.get("title")) for badge in badges]
                return "\n".join(badges_list)
            return ""
        except Exception as err:
            logger.warning(f"Ошибка при парсинге характеристик пользователя: {err}")
            return ""

    async def _extract_seller_info(self, ad_json: dict, seller_json: dict | None) -> Seller | None:
        try:
            seller_info = ad_json.get("seller", {})
            seller = Seller(
                name=seller_info.get("name", None),
                experience=self._extract_experience(ad_json),
                characteristics=self._extract_characteristics(ad_json),
                type="Компания" if ad_json.get("favoriteSeller", {}).get("isShop") else "Частный исполнитель",
            )

            if registration_date := seller_info.get("tenureSince", ""):
                registration_date = dateparser.parse(registration_date)
                registration_date = registration_date.replace(day=1).strftime("%Y-%m-%d")
                seller.registration_date = registration_date

            if seller_json:
                item_data = seller_json.get("searchData", {}).get("profileCatalog", {})
                try:
                    seller.completed_ad_count = 0 if "нет" in item_data.get("items", [])[0].get("closedItemsText",
                                                                                                "0").lower() else int(
                        item_data.get("items", [])[0].get("closedItemsText", "0").split()[0])
                    seller.active_ad_count = item_data.get("foundCount", 0)
                except Exception as err:
                    seller.completed_ad_count = 0
                    seller.active_ad_count = 0

            return seller
        except Exception as err:
            logger.error(f"Ошибка при парсинге инфрмации о продавце {err}/{traceback.format_exc()}")
            return None

    async def _extract_reviews(self, ad: Item, seller_json: dict) -> tuple[str, int] | None:
        try:
            if review_data := seller_json.get("reviewsData", {}).get("entries", []):
                count_reviews: int = review_data[0].get("value").get("reviewCount", 0)

                reviews = []
                entries = list(filter(
                    lambda entry: entry.get("type") == "rating" and entry.get("value", {}).get("itemTitle") == ad.title,
                    review_data
                ))

                for entry in entries:
                    review_entry = entry.get("value")
                    review = Review(
                        text=review_entry.get("textSections")[0].get("text"),
                        author=review_entry.get("title"),
                        score=review_entry.get("score"),
                    )
                    if review_date := dateparser.parse(review_entry.get("rated")):
                        review_date = review_date.strftime("%Y-%m-%d")
                        review.date = review_date

                    if answer := review_entry.get('answer'):
                        answer = AnswerOnReview(
                            text=answer.get("text"),
                            author=answer.get("title")
                        )
                        review.answer = answer

                    reviews_to_text = f"Дата: {review.date}\nАвтор: {review.author}\nОтзыв: {review.text}\nОценка: {review.score}"
                    if review.answer:
                        reviews_to_text += f"\nОтвет:{review.answer.text}"

                    reviews.append(reviews_to_text)
                return "\n\n".join(reviews), count_reviews
            return None, None
        except Exception as err:
            logger.warning(f"Ошибка при парсинге отзывов {err}")
            return None, None

    def _extract_score(self, ad_json: dict) -> float | None:
        try:
            rating = ad_json.get("rating", {})
            score = rating.get("scoreFloat", {})
            return float(score)
        except Exception as err:
            logger.warning(f"Ошибка при парсинге общей оценки отзывов: {err}")
            return None

    @staticmethod
    def _extract_views(html: str) -> tuple | None:
        try:
            soup = BeautifulSoup(html, "html.parser")

            def extract_digits(element):
                return int(''.join(filter(str.isdigit, element.get_text()))) if element else None

            total = extract_digits(soup.select_one('[data-marker="item-view/total-views"]'))
            today = extract_digits(soup.select_one('[data-marker="item-view/today-views"]'))

            return total, today
        except Exception as err:
            logger.warning("Ошибка при получении числа просмотров")
            return None

    def _extract_specification(self, ad_json: dict) -> str | None:
        try:
            categories = ad_json.get("item", {}).get("breadcrumbs", [])
            last_category = categories[-1]
            specification = last_category.get("title")
            return specification
        except Exception as err:
            logger.warning(f"Ошибка во время парсинга категории {err}")
            return None

    @staticmethod
    def _extract_seller_slug(data):
        match = re.search(r"/brands/([^/?#]+)", str(data))
        if match:
            return match.group(1)
        return None

    def is_viewed(self, ad: Item) -> bool:
        """Проверяет, смотрели мы это или нет"""
        return self.db_handler.record_exists(record_id=ad.id, price=ad.priceDetailed.value)

    @staticmethod
    def _is_recent(timestamp_ms: int, max_age_seconds: int) -> bool:
        now = datetime.utcnow()
        published_time = datetime.utcfromtimestamp(timestamp_ms / 1000)
        return (now - published_time) <= timedelta(seconds=max_age_seconds)

    def __save_viewed(self, ads: list[Item]) -> None:
        """Сохраняет просмотренные объявления"""
        try:
            self.db_handler.add_record_from_page(ads=ads)
        except Exception as err:
            logger.info(f"При сохранении в БД ошибка {err}")

    def get_next_page_url(self, url: str):
        """Получает следующую страницу"""
        try:
            url_parts = urlparse(url)
            query_params = parse_qs(url_parts.query)
            current_page = int(query_params.get('p', [1])[0])
            query_params['p'] = current_page + 1
            if self.config.one_time_start:
                logger.debug(f"Страница {current_page}")

            new_query = urlencode(query_params, doseq=True)
            next_url = urlunparse((url_parts.scheme, url_parts.netloc, url_parts.path, url_parts.params, new_query,
                                   url_parts.fragment))
            return next_url
        except Exception as err:
            logger.error(f"Не смог сформировать ссылку на следующую страницу для {url}. Ошибка: {err}")


async def main():
    try:
        config = load_avito_config("config.toml")
    except Exception as err:
        logger.error(f"Ошибка загрузки конфига: {err}")
        exit(1)

    while True:
        try:
            parser = AvitoParse(config)
            await parser.parse()
            if config.one_time_start:
                logger.info("Парсинг завершен т.к. включён one_time_start в настройках")
                break
            logger.info(f"Парсинг завершен. Пауза {config.pause_general} сек")
            time.sleep(config.pause_general)
        except Exception as err:
            logger.exception(err)
            logger.error(f"Произошла ошибка {err}. Будет повторный запуск через 30 сек.")
            time.sleep(30)


if __name__ == "__main__":
    asyncio.run(main())
