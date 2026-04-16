# src/data/_http.py
import asyncio
import logging
import random
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
]

_HEADERS_BASE = {
    'Accept': '*/*',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'Connection': 'keep-alive',
    'Referer': 'https://gu.qq.com/',
}


def get_random_ua() -> str:
    return random.choice(USER_AGENTS)


async def fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    max_retries: int = 3,
    timeout: int = 10,
) -> Optional[str]:
    """带指数退避重试的 GET 请求，返回响应文本或 None。"""
    for attempt in range(max_retries):
        try:
            headers = {**_HEADERS_BASE, 'User-Agent': get_random_ua()}
            client_timeout = aiohttp.ClientTimeout(total=timeout)
            async with session.get(url, headers=headers, timeout=client_timeout) as response:
                if response.status == 200:
                    return await response.text()
                logger.debug('非200响应 status=%d (尝试 %d/%d): %s', response.status, attempt + 1, max_retries, url[:80])
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5 * (2 ** attempt))
        except asyncio.TimeoutError:
            logger.debug('请求超时 (尝试 %d/%d): %s...', attempt + 1, max_retries, url[:80])
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5 * (2 ** attempt))
        except Exception as exc:
            logger.debug('请求失败 (尝试 %d/%d): %s', attempt + 1, max_retries, exc)
            if attempt < max_retries - 1:
                await asyncio.sleep(0.5 * (2 ** attempt))
    return None
