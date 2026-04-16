import pytest
from src.data._http import USER_AGENTS, get_random_ua, fetch_with_retry


def test_user_agents_not_empty():
    assert len(USER_AGENTS) >= 3


def test_get_random_ua_returns_string():
    ua = get_random_ua()
    assert isinstance(ua, str)
    assert 'Mozilla' in ua


def test_get_random_ua_from_pool():
    ua = get_random_ua()
    assert ua in USER_AGENTS


@pytest.mark.asyncio
async def test_fetch_with_retry_returns_none_on_all_failures():
    """fetch_with_retry 全部重试失败时返回 None"""
    import aiohttp
    from unittest.mock import AsyncMock, MagicMock, patch

    mock_response = MagicMock()
    mock_response.status = 500
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=mock_response)

    with patch('src.data._http.asyncio.sleep', new_callable=AsyncMock):
        result = await fetch_with_retry(mock_session, 'http://example.com', max_retries=2, timeout=1)

    assert result is None


@pytest.mark.asyncio
async def test_fetch_with_retry_returns_text_on_success():
    import aiohttp
    from unittest.mock import AsyncMock, MagicMock

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value='ok_content')
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.get = MagicMock(return_value=mock_response)

    result = await fetch_with_retry(mock_session, 'http://example.com', max_retries=1, timeout=1)
    assert result == 'ok_content'
