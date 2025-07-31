# 声明：本代码仅供学习和研究目的使用。使用者应遵守以下原则：
# 1. 不得用于任何商业用途。
# 2. 使用时应遵守目标平台的使用条款和robots.txt规则。
# 3. 不得进行大规模爬取或对平台造成运营干扰。
# 4. 应合理控制请求频率，避免给目标平台带来不必要的负担。
# 5. 不得用于任何非法或不当的用途。
#
# 详细许可条款请参阅项目根目录下的LICENSE文件。
# 使用本代码即表示您同意遵守上述原则和LICENSE中的所有条款。


# -*- coding: utf-8 -*-
# @Author  : relakkes@gmail.com
# @Time    : 2023/12/23 15:41
# @Desc    : 微博爬虫主流程代码


import asyncio
import html
import os
import random
import re
from asyncio import Task
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import quote_plus

import httpx
from playwright.async_api import (
    BrowserContext,
    BrowserType,
    Page,
    Playwright,
    async_playwright,
)

import config
from base.base_crawler import AbstractCrawler
from proxy.proxy_ip_pool import IpInfoModel, create_ip_pool
from store import weibo as weibo_store
from tools import utils
from tools.cdp_browser import CDPBrowserManager
from var import crawler_type_var, source_keyword_var

from .client import WeiboClient
from .exception import DataFetchError
from .field import SearchType
from .help import filter_search_result_card
from .login import WeiboLogin

# ============================================================
#  核心爬虫
# ============================================================


class WeiboCrawler(AbstractCrawler):
    # --------------------------------------------------------
    context_page: Page
    wb_client: WeiboClient
    browser_context: BrowserContext
    cdp_manager: Optional[CDPBrowserManager]

    def __init__(self):
        self.mobile_index_url = "https://m.weibo.cn"
        self.desktop_index_url = "https://www.weibo.com"
        self.user_agent = utils.get_user_agent()
        self.mobile_user_agent = utils.get_mobile_user_agent()
        self.cdp_manager = None

        self._seen_mid_set: Set[str] = set()  # 跨小时去重
        # 桌面域 cookies
        self.weibo_com_cookie_str: str = ""
        self.weibo_com_cookie_dict: Dict[str, str] = {}
        
        self.search_mode = getattr(config, "SEARCH_MODE", "normal")
        self.start_time  = getattr(config, "START_TIME", "").strip()
        self.end_time    = getattr(config, "END_TIME", "").strip()
        self.page_delay  = getattr(config, "PAGE_DELAY", "")
        self.comment_delay  = getattr(config, "COMMENT_DELAY", "")

    # =======================================================
    #  启动
    # =======================================================
    async def start(self):
        ply_proxy, httpx_proxy = None, None
        if config.ENABLE_IP_PROXY:
            pool = await create_ip_pool(config.IP_PROXY_POOL_COUNT, True)
            ip_info: IpInfoModel = await pool.get_proxy()
            ply_proxy, httpx_proxy = utils.format_proxy_info(ip_info)

        async with async_playwright() as pw:
            # ---------- 浏览器 ----------
            if config.ENABLE_CDP_MODE:
                self.browser_context = await self.launch_browser_with_cdp(
                    pw,
                    ply_proxy,
                    self.mobile_user_agent,
                    headless=config.CDP_HEADLESS,
                )
            else:
                chromium: BrowserType = pw.chromium
                self.browser_context = await self.launch_browser(
                    chromium, None, self.mobile_user_agent, headless=config.HEADLESS
                )

            await self.browser_context.add_init_script(path="libs/stealth.min.js")
            self.context_page = await self.browser_context.new_page()
            await self.context_page.goto(self.mobile_index_url)

            # ---------- 移动端客户端 ----------
            self.wb_client = await self.create_weibo_client(httpx_proxy)

            # ---------- 移动端登录 ----------
            if not await self.wb_client.pong():
                login_m = WeiboLogin(
                    login_type=config.LOGIN_TYPE,
                    browser_context=self.browser_context,
                    context_page=self.context_page,
                    cookie_str=config.COOKIES,
                )
                await login_m.begin()
                await self.context_page.goto(self.mobile_index_url)
                await asyncio.sleep(2)
                await self.wb_client.update_cookies(self.browser_context)

            # ---------- 如 timerange 模式需登录桌面端 ----------
            if self.search_mode == "timerange":
                await self._ensure_desktop_cookie()

            # ---------- 进入业务流程 ----------
            crawler_type_var.set(config.CRAWLER_TYPE)
            if config.CRAWLER_TYPE == "search":
                await self.search()
            elif config.CRAWLER_TYPE == "detail":
                await self.get_specified_notes()
            elif config.CRAWLER_TYPE == "creator":
                await self.get_creators_and_notes()

            utils.logger.info("[WeiboCrawler] finished")

    # ---------------- 桌面端登录 ----------------
    async def _ensure_desktop_cookie(self):
        """
        timerange 模式下确保 s.weibo.com 可用。
        先访问一次搜索页 -> 若仍要求登录则弹出二维码扫码。
        """
        if self.weibo_com_cookie_str:   # 已处理过
            return
    
        desk_page = await self.browser_context.new_page()
        await desk_page.set_extra_http_headers({"User-Agent": self.user_agent})
    
        async def _is_logged_in() -> bool:
            """访问搜索页，判断是否被跳登陆"""
            resp = await desk_page.goto(
                "https://s.weibo.com/weibo?q=登录检测&Refer=search_home", wait_until="domcontentloaded"
            )
            # 1) 若 302，Playwright 会跳到最终地址；取 page.url
            final_url = desk_page.url
            if "login.php" in final_url or "passport.weibo.com" in final_url:
                return False
            # 2) 若返回 html，但包含登录表单，也视为未登录
            content = await desk_page.content()
            return "pl_login_form" not in content
    
        # 第一次检测
        if not await _is_logged_in():
            utils.logger.info("[WeiboLogin] s.weibo.com 未登录，弹出二维码 ...")
            # 尝试点击 “登录 / 立即登录” 按钮，如果页面需要
            try:
                await desk_page.click("text=登录", timeout=3000)
            except Exception:
                pass
            # 调用二维码登录
            login_d = WeiboLogin(
                login_type=config.LOGIN_TYPE,
                browser_context=self.browser_context,
                context_page=desk_page,
            )
            await login_d.login_desktop(desk_page)
    
            # 再访问一次确认
            if not await _is_logged_in():
                raise RuntimeError("桌面端扫码后仍未登录成功，请检查账号状态")
    
        # 保存最终 weibo.com cookie
        ck_str, ck_dict = utils.convert_cookies(await self.browser_context.cookies())
        self.weibo_com_cookie_str = ck_str
        self.weibo_com_cookie_dict = ck_dict
        utils.logger.info("[WeiboCrawler] weibo.com cookies OK，开始 timescope 抓取")

    # =======================================================
    #  关键词搜索
    # =======================================================
    async def search(self):
        utils.logger.info("[WeiboCrawler.search] 开始搜索关键词")
        limit_per_page = 10
        if config.CRAWLER_MAX_NOTES_COUNT < limit_per_page:
            config.CRAWLER_MAX_NOTES_COUNT = limit_per_page
        start_page = config.START_PAGE

        stype_map = {
            "default": SearchType.DEFAULT,
            "real_time": SearchType.REAL_TIME,
            "popular": SearchType.POPULAR,
            "video": SearchType.VIDEO,
        }
        search_type = stype_map.get(config.WEIBO_SEARCH_TYPE, SearchType.DEFAULT)

        for kw in config.KEYWORDS.split(","):
            source_keyword_var.set(kw)
            if self.search_mode == "timerange":
                if not (self.start_time and self.end_time):
                    utils.logger.error("timerange 模式需同时指定起止时间")
                    return
                await self._search_timerange(kw)
            else:
                await self._search_normal(kw, start_page, limit_per_page, search_type)

    # ---------------- normal ----------------
    async def _search_normal(self, kw: str, start_page: int, per_page: int, stype: SearchType):
        page = 1
        while (page - start_page + 1) * per_page <= config.CRAWLER_MAX_NOTES_COUNT:
            if page < start_page:
                page += 1
                continue
            res = await self.wb_client.get_note_by_keyword(kw, page, stype)
            ids, cards = [], filter_search_result_card(res.get("cards"))
            for card in cards:
                mblog = card.get("mblog")
                if mblog:
                    ids.append(mblog["id"])
                    await weibo_store.update_weibo_note(card)
                    await self.get_note_images(mblog)
            page += 1
            await asyncio.sleep(self.page_delay)
            await self.batch_get_notes_comments(ids)

    # ---------------- timerange ----------------
    async def _search_timerange(self, kw: str):
        def _p(s):
            try:
                return datetime.strptime(s, "%Y-%m-%d %H")
            except ValueError:
                return datetime.strptime(s, "%Y-%m-%d")

        b_dt, e_dt = _p(self.start_time), _p(self.end_time)
        if e_dt.hour == 0 and e_dt.minute == 0 and e_dt.second == 0:
            e_dt += timedelta(days=1)

        async with httpx.AsyncClient(
            headers={
                "User-Agent": self.user_agent,
                "Referer": "https://s.weibo.com/",
                "Cookie": self.weibo_com_cookie_str,
            },
            cookies=self.weibo_com_cookie_dict,
            timeout=self.wb_client.timeout,
        ) as client:
            cur = b_dt
            while cur < e_dt:
                nxt = cur + timedelta(hours=1)
                ts_s = f"{cur:%Y-%m-%d}-{cur.hour}"
                ts_e = f"{nxt:%Y-%m-%d}-{nxt.hour}"
                utils.logger.info(
                    f"[WeiboCrawler] timescope {ts_s} → {ts_e} | kw={kw}"
                )
                await self._crawl_timescope(client, kw, ts_s, ts_e)
                cur = nxt

    async def _crawl_timescope(self, client, kw: str, ts_s: str, ts_e: str):
        """
        抓取单个 ≤1 小时窗口：
          - 只要当前页提取到 mid，就继续 page += 1
          - 连续 2 页空结果即认为到尾
          - 最多抓 50 页防止死循环
        """
        page, empty_seq, batch = 1, 0, []
        tpl = (
            "https://s.weibo.com/weibo?q={}&typeall=1&suball=1"
            "&timescope=custom:{}:{}&Refer=g&page={}"
        )
    
        while page <= 50:                 # 上限 50 页
            url = tpl.format(quote_plus(kw), ts_s, ts_e, page)
            resp = await client.get(url)
            mids = re.findall(r'\bmid="(\d{8,})"', html.unescape(resp.text))
    
            utils.logger.info(
                f"[WeiboCrawler] {ts_s} → {ts_e} | page {page} | mids {len(mids)}"
            )
    
            if not mids:                   # 空页
                empty_seq += 1
                if empty_seq >= 2:
                    break                 # 连续两页空 → 结束窗口
                page += 1
                continue
            else:
                empty_seq = 0              # 重置空页计数
    
            for mid in mids:
                if mid in self._seen_mid_set:
                    continue
                self._seen_mid_set.add(mid)
                try:
                    detail = await self.wb_client.get_note_info_by_id(mid)
                except DataFetchError as e:
                    utils.logger.warning(f"mid {mid} failed: {e}")
                    continue
    
                if detail and detail.get("mblog"):
                    mblog = detail["mblog"]
                    await weibo_store.update_weibo_note(detail)
                    await self.get_note_images(mblog)
                    batch.append(mid)
    
            page += 1                      # 翻到下一页
            await asyncio.sleep(self.page_delay)
        await self.batch_get_notes_comments(batch)

    @staticmethod
    def _get_page_cnt(html_text: str) -> int:
        try:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(html_text, "lxml")
            ul = soup.find("ul", class_="s-scroll")
            return min(len(ul.find_all("li")) if ul else 1, 50)
        except Exception:
            return 1

    async def get_specified_notes(self):
        """
        get specified notes info
        :return:
        """
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list = [
            self.get_note_info_task(note_id=note_id, semaphore=semaphore)
            for note_id in config.WEIBO_SPECIFIED_ID_LIST
        ]
        video_details = await asyncio.gather(*task_list)
        for note_item in video_details:
            if note_item:
                await weibo_store.update_weibo_note(note_item)
        await self.batch_get_notes_comments(config.WEIBO_SPECIFIED_ID_LIST)

    async def get_note_info_task(
        self, note_id: str, semaphore: asyncio.Semaphore
    ) -> Optional[Dict]:
        """
        Get note detail task
        :param note_id:
        :param semaphore:
        :return:
        """
        async with semaphore:
            try:
                result = await self.wb_client.get_note_info_by_id(note_id)
                return result
            except DataFetchError as ex:
                utils.logger.error(
                    f"[WeiboCrawler.get_note_info_task] Get note detail error: {ex}"
                )
                return None
            except KeyError as ex:
                utils.logger.error(
                    f"[WeiboCrawler.get_note_info_task] have not fund note detail note_id:{note_id}, err: {ex}"
                )
                return None

    async def batch_get_notes_comments(self, note_id_list: List[str]):
        """
        batch get notes comments
        :param note_id_list:
        :return:
        """
        if not config.ENABLE_GET_COMMENTS:
            utils.logger.info(
                f"[WeiboCrawler.batch_get_note_comments] Crawling comment mode is not enabled"
            )
            return

        utils.logger.info(
            f"[WeiboCrawler.batch_get_notes_comments] note ids:{note_id_list}"
        )
        semaphore = asyncio.Semaphore(config.MAX_CONCURRENCY_NUM)
        task_list: List[Task] = []
        for note_id in note_id_list:
            task = asyncio.create_task(
                self.get_note_comments(note_id, semaphore), name=note_id
            )
            task_list.append(task)
        await asyncio.gather(*task_list)

    async def get_note_comments(self, note_id: str, semaphore: asyncio.Semaphore):
        """
        get comment for note id
        :param note_id:
        :param semaphore:
        :return:
        """
        async with semaphore:
            try:
                utils.logger.info(
                    f"[WeiboCrawler.get_note_comments] begin get note_id: {note_id} comments ..."
                )
                await self.wb_client.get_note_all_comments(
                    note_id=note_id,
                    crawl_interval=self.comment_delay,
                    callback=weibo_store.batch_update_weibo_note_comments,
                    max_count=config.CRAWLER_MAX_COMMENTS_COUNT_SINGLENOTES,
                )
            except DataFetchError as ex:
                utils.logger.error(
                    f"[WeiboCrawler.get_note_comments] get note_id: {note_id} comment error: {ex}"
                )
            except Exception as e:
                utils.logger.error(
                    f"[WeiboCrawler.get_note_comments] may be been blocked, err:{e}"
                )

    async def get_note_images(self, mblog: Dict):
        """
        get note images
        :param mblog:
        :return:
        """
        if not config.ENABLE_GET_IMAGES:
            utils.logger.info(
                f"[WeiboCrawler.get_note_images] Crawling image mode is not enabled"
            )
            return

        pics: Dict = mblog.get("pics")
        if not pics:
            return
        for pic in pics:
            url = pic.get("url")
            if not url:
                continue
            content = await self.wb_client.get_note_image(url)
            if content != None:
                extension_file_name = url.split(".")[-1]
                await weibo_store.update_weibo_note_image(
                    pic["pid"], content, extension_file_name
                )

    async def get_creators_and_notes(self) -> None:
        """
        Get creator's information and their notes and comments
        Returns:

        """
        utils.logger.info(
            "[WeiboCrawler.get_creators_and_notes] Begin get weibo creators"
        )
        for user_id in config.WEIBO_CREATOR_ID_LIST:
            createor_info_res: Dict = await self.wb_client.get_creator_info_by_id(
                creator_id=user_id
            )
            if createor_info_res:
                createor_info: Dict = createor_info_res.get("userInfo", {})
                utils.logger.info(
                    f"[WeiboCrawler.get_creators_and_notes] creator info: {createor_info}"
                )
                if not createor_info:
                    raise DataFetchError("Get creator info error")
                await weibo_store.save_creator(user_id, user_info=createor_info)

                # Get all note information of the creator
                all_notes_list = await self.wb_client.get_all_notes_by_creator_id(
                    creator_id=user_id,
                    container_id=createor_info_res.get("lfid_container_id"),
                    crawl_interval=0,
                    callback=weibo_store.batch_update_weibo_notes,
                )

                note_ids = [
                    note_item.get("mblog", {}).get("id")
                    for note_item in all_notes_list
                    if note_item.get("mblog", {}).get("id")
                ]
                await self.batch_get_notes_comments(note_ids)

            else:
                utils.logger.error(
                    f"[WeiboCrawler.get_creators_and_notes] get creator info error, creator_id:{user_id}"
                )

    # ---------------- 浏览器 / 客户端 ----------------
    async def create_weibo_client(self, httpx_proxy: Optional[str]) -> WeiboClient:
        ck_str, ck_dict = utils.convert_cookies(await self.browser_context.cookies())
        return WeiboClient(
            proxies=httpx_proxy,
            headers={
                "User-Agent": utils.get_mobile_user_agent(),
                "Cookie": ck_str,
                "Origin": "https://m.weibo.cn",
                "Referer": "https://m.weibo.cn",
                "Content-Type": "application/json;charset=UTF-8",
            },
            playwright_page=self.context_page,
            cookie_dict=ck_dict,
        )

    @staticmethod
    def format_proxy_info(ip: IpInfoModel) -> Tuple[Optional[Dict], Optional[Dict]]:
        return utils.format_proxy_info(ip)

    async def launch_browser(
        self, chromium: BrowserType, proxy: Optional[Dict], ua: str, headless=True
    ) -> BrowserContext:
        if config.SAVE_LOGIN_STATE:
            ud_dir = os.path.join(os.getcwd(), "browser_data", config.USER_DATA_DIR % config.PLATFORM)
            return await chromium.launch_persistent_context(
                user_data_dir=ud_dir,
                accept_downloads=True,
                headless=headless,
                proxy=proxy,  # type: ignore
                viewport={"width": 1920, "height": 1080},
                user_agent=ua,
            )
        browser = await chromium.launch(headless=headless, proxy=proxy)  # type: ignore
        return await browser.new_context(viewport={"width": 1920, "height": 1080}, user_agent=ua)

    async def launch_browser_with_cdp(
        self, pw: Playwright, proxy: Optional[Dict], ua: str, headless=True
    ):
        try:
            self.cdp_manager = CDPBrowserManager()
            return await self.cdp_manager.launch_and_connect(
                playwright=pw, playwright_proxy=proxy, user_agent=ua, headless=headless
            )
        except Exception as e:
            utils.logger.error(f"[WeiboCrawler] CDP 启动失败回退: {e}")
            return await self.launch_browser(pw.chromium, proxy, ua, headless)

    async def close(self):
        if self.cdp_manager:
            await self.cdp_manager.cleanup()
            self.cdp_manager = None
        else:
            await self.browser_context.close()
        utils.logger.info("[WeiboCrawler.close] Browser context closed ...")
