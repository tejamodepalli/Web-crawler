"""
Scheduler: the periodic tick that decides what to crawl next.

"The scheduler checks which sources are due for crawling and creates the
required tasks" (design doc). Runs as a Celery beat task on a fixed
interval (SCHEDULER_TICK_SECONDS, default 30s). For each enabled source:
  - skip if a job for it is already in progress
  - skip if it isn't due yet
  - acquire a Redis lock to prevent duplicate jobs
  - enqueue a crawl job on the queue matching its priority
"""

import logging

from engine.celery_app import app
from engine.sources import load_sources
from engine.state import CrawlState
from engine.tasks import crawl_source


logger = logging.getLogger(__name__)

state = CrawlState()


@app.task
def check_and_schedule():
    scheduled = []

    for source in load_sources():

        # Skip disabled sources
        if not source.get("enabled", True):
            continue

        name = source.get("name", "").strip()
        url = source.get("url", "").strip()
        priority = source.get("priority", "").strip()
        interval_minutes = source.get("interval_minutes")

        # Validate required source fields
        if not name or not url:
            logger.warning(
                "Skipping invalid source: missing name or URL"
            )
            continue

        # Validate crawl interval
        try:
            interval_minutes = int(interval_minutes)
        except (TypeError, ValueError):
            logger.warning(
                "Skipping '%s': invalid interval_minutes=%r",
                name,
                interval_minutes,
            )
            continue

        if interval_minutes <= 0:
            logger.warning(
                "Skipping '%s': interval_minutes must be greater than 0",
                name,
            )
            continue

        # Validate queue priority
        if priority not in {"fast", "standard", "bulk"}:
            logger.warning(
                "Skipping '%s': invalid priority '%s'",
                name,
                priority,
            )
            continue

        # Skip if this source is already being crawled
        if state.is_in_progress(name):
            logger.debug(
                "Skipping '%s': already in progress",
                name
            )
            continue

        # Skip if the source is not due yet
        if not state.is_due(name, interval_minutes):
            continue

        # Atomically claim the source before putting the job
        # into the Celery queue.
        if not state.acquire_lock(name):
            logger.debug(
                "Skipping '%s': already claimed",
                name
            )
            continue

        crawl_source.apply_async(
            args=[source],
            queue=priority,
        )

        scheduled.append(name)

        logger.info(
            "Scheduled crawl for '%s' on queue '%s'",
            name,
            priority,
        )

    return {
        "scheduled": scheduled
    }