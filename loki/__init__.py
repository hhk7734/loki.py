from datetime import datetime, timezone
from enum import Enum
from urllib.parse import urljoin

import aiohttp

LOKI_V1_QUERY_RANGE = "/loki/api/v1/query_range"


class Direction(str, Enum):
    FORWARD = "forward"
    BACKWARD = "backward"


class Client:
    def __init__(self, url: str) -> None:
        self._url = url

    async def queryRange(
        self,
        query: str,
        limit: int | None = None,
        start: datetime | None = None,
        end: datetime | None = None,
        direction: str | Direction | None = None,
    ):
        """
        https://grafana.com/docs/loki/latest/api/#query-loki-over-a-range-of-time

        Args:
            query (str): LogQL query
            limit (int | None, optional): The max number of entries to return. It defaults to 100.
                Only applies to query types which produce a stream(log lines) response.
            start (datetime | None, optional): The start time for the query. Defaults to one hour
                ago.
            end (datetime | None, optional): The end time for the query. Defaults to now.
            direction (str | Direction | None, optional): Determines the sort order of logs.
                Defaults to "backward".

        Returns:
            {
                "status": "success",
                "data": {
                    "resultType": "matrix" | "streams",
                    "result": [<matrix value>] | [<stream value>],
                    "stats" : [<statistics>]
                }
            }
        """
        params = {"query": query}
        if limit:
            params["limit"] = str(limit)
        if start:
            params["start"] = start.astimezone(tz=timezone.utc).isoformat().replace("+00:00", "Z")
        if end:
            params["end"] = end.astimezone(tz=timezone.utc).isoformat().replace("+00:00", "Z")
        if direction:
            params["direction"] = Direction(direction).value
        async with aiohttp.ClientSession() as session:
            async with session.get(
                urljoin(self._url, LOKI_V1_QUERY_RANGE), params=params
            ) as response:
                return await response.json()
