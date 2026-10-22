 """
Single-source spider.

Week 6:
- Start from one authorized seed URL.
- Fetch the seed through the configured gateway.
- Follow links on the same host.
- Limit the number of pages crawled per job.
- Produce one crawl-result record per fetched page.
"""

from datetime import datetime, timezone
from urllib.parse import urlparse

import scrapy

from Crawler.items import CrawlResultItem


class SingleSourceSpider(scrapy.Spider):
    name = "single_source"

    custom_settings = {
        # Keep requests polite for each source.
        "CONCURRENT_REQUESTS_PER_DOMAIN": 1,

        # Prevent unlimited crawling depth.
        "DEPTH_LIMIT": 10,
    }

    def __init__(
        self,
        url=None,
        source_name="source_1",
        source_type="normal",
        max_pages=10,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if not url:
            raise ValueError("Provide the authorized seed URL: -a url=<...>")

        self.start_urls = [url]
        self.source_name = source_name
        self.source_type = source_type

        try:
            self.max_pages = int(max_pages)
        except (TypeError, ValueError):
            raise ValueError("max_pages must be a positive integer")

        if self.max_pages <= 0:
            raise ValueError("max_pages must be greater than 0")

        parsed_url = urlparse(url)
        self.allowed_host = (parsed_url.hostname or "").lower()

        if not self.allowed_host:
            raise ValueError("The seed URL must contain a valid hostname")

        self.pages_crawled = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                callback=self.parse,
                errback=self.handle_failure,
                meta={
                    "attempt": 1,
                    "depth": 0,
                },
                dont_filter=True,
            )

    def parse(self, response):
        # Count this fetched page.
        self.pages_crawled += 1

        title = response.xpath("//title/text()").get(default="").strip()

        text_content = " ".join(
            t.strip()
            for t in response.xpath("//body//text()").getall()
            if t.strip()
        )

        item = CrawlResultItem()
        item["source_name"] = self.source_name
        item["source_type"] = self.source_type
        item["url"] = response.url
        item["network"] = response.meta.get("network", "clearnet")
        item["fetch_timestamp"] = datetime.now(timezone.utc).isoformat()
        item["crawler_build"] = self.settings.get("CRAWLER_BUILD")
        item["status"] = "success"
        item["http_status"] = response.status
        item["attempt"] = response.meta.get("retry_times", 0) + 1
        item["title"] = title
        item["raw_html"] = response.text
        item["text_content"] = text_content

        yield item

        # Stop when the configured page limit is reached.
        if self.pages_crawled >= self.max_pages:
            self.logger.info(
                "Maximum page limit reached: %s pages",
                self.max_pages,
            )
            return

        # Follow links only on the same authorized host.
        for href in response.css("a::attr(href)").getall():
            next_url = response.urljoin(href)

            parsed_next = urlparse(next_url)

            # Only follow HTTP/HTTPS URLs.
            if parsed_next.scheme not in {"http", "https"}:
                continue

            # Stay on the same host.
            next_host = (parsed_next.hostname or "").lower()

            if next_host != self.allowed_host:
                continue

            yield scrapy.Request(
                next_url,
                callback=self.parse,
                errback=self.handle_failure,
                meta={
                    "attempt": 1,
                    "depth": response.meta.get("depth", 0) + 1,
                },
            )

    def handle_failure(self, failure):
        request = failure.request

        # Count failed requests toward the crawl limit.
        self.pages_crawled += 1

        self.logger.error(
            "Fetch failed permanently for %s: %s",
            request.url,
            failure.value,
        )

        response = getattr(failure.value, "response", None)

        item = CrawlResultItem()
        item["source_name"] = self.source_name
        item["source_type"] = self.source_type
        item["url"] = request.url
        item["network"] = request.meta.get("network", "clearnet")
        item["fetch_timestamp"] = datetime.now(timezone.utc).isoformat()
        item["crawler_build"] = self.settings.get("CRAWLER_BUILD")
        item["status"] = "failed"
        item["http_status"] = response.status if response else None
        item["attempt"] = request.meta.get("retry_times", 0) + 1
        item["title"] = ""
        item["raw_html"] = ""
        item["text_content"] = ""

        yield item
