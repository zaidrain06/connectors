import logging
from datetime import date
from unittest.mock import AsyncMock, MagicMock

import pytest

from source_apple_app_store.api import _stream_analytics_report_data
from source_apple_app_store.client import API_BASE, AppleAppStoreClient
from source_apple_app_store.models import (
    AnalyticsReportGranularity,
    AnalyticsReportInstance,
    AnalyticsReportSegment,
    AppCrashesReportRow,
)


@pytest.mark.asyncio
async def test_get_analytics_report_segment_details() -> None:
    log = logging.getLogger("test")
    http = MagicMock()
    http.request = AsyncMock(
        return_value=b'{"data":{"id":"seg-123","attributes":{"checksum":"abc123","sizeInBytes":2048,"url":"https://fresh.example.com/reports/seg-123.txt.gz?X-Amz-Expires=300"}}}'
    )

    client = object.__new__(AppleAppStoreClient)
    client.log = log
    client.http = http

    segment = await client.get_analytics_report_segment_details("seg-123")

    http.request.assert_awaited_once_with(
        log,
        f"{API_BASE}/analyticsReportSegments/seg-123",
    )
    assert segment.id == "seg-123"
    assert segment.attributes.url == "https://fresh.example.com/reports/seg-123.txt.gz?X-Amz-Expires=300"
    assert segment.filename == "reports/seg-123.txt.gz"


@pytest.mark.asyncio
async def test_stream_analytics_report_data_fetches_fresh_segment_url_before_download() -> None:
    log = logging.getLogger("test")
    client = MagicMock()

    instance = AnalyticsReportInstance(
        id="inst-1",
        attributes=AnalyticsReportInstance.Attributes(
            granularity=AnalyticsReportGranularity.DAILY,
            processingDate=date(2026, 11, 4),
        ),
    )

    stale_segment_1 = AnalyticsReportSegment(
        id="seg-1",
        attributes=AnalyticsReportSegment.Attributes(
            url="https://stale.example.com/seg-1.txt.gz?X-Amz-Expires=300",
        ),
    )
    stale_segment_2 = AnalyticsReportSegment(
        id="seg-2",
        attributes=AnalyticsReportSegment.Attributes(
            url="https://stale.example.com/seg-2.txt.gz?X-Amz-Expires=300",
        ),
    )

    fresh_segment_1 = AnalyticsReportSegment(
        id="seg-1",
        attributes=AnalyticsReportSegment.Attributes(
            url="https://fresh.example.com/seg-1.txt.gz?X-Amz-Expires=300",
        ),
    )
    fresh_segment_2 = AnalyticsReportSegment(
        id="seg-2",
        attributes=AnalyticsReportSegment.Attributes(
            url="https://fresh.example.com/seg-2.txt.gz?X-Amz-Expires=300",
        ),
    )

    call_order: list[str] = []

    async def mock_get_segment_details(segment_id: str) -> AnalyticsReportSegment:
        call_order.append(f"fetch:{segment_id}")
        return fresh_segment_1 if segment_id == "seg-1" else fresh_segment_2

    async def mock_stream_tsv_data(
        filename: str,
        download_url: str,
        model: type[AppCrashesReportRow],
    ):
        call_order.append(f"download:{download_url}")
        yield {"segment": filename, "url": download_url}

    client.list_analytics_report_segments = AsyncMock(
        return_value=[stale_segment_1, stale_segment_2]
    )
    client.get_analytics_report_segment_details = AsyncMock(
        side_effect=mock_get_segment_details
    )
    client.stream_tsv_data = mock_stream_tsv_data

    rows = [
        row
        async for row in _stream_analytics_report_data(
            log=log,
            client=client,
            app_id="app-1",
            model=AppCrashesReportRow,
            instance=instance,
        )
    ]

    assert len(rows) == 2
    assert call_order == [
        "fetch:seg-1",
        "download:https://fresh.example.com/seg-1.txt.gz?X-Amz-Expires=300",
        "fetch:seg-2",
        "download:https://fresh.example.com/seg-2.txt.gz?X-Amz-Expires=300",
    ]
