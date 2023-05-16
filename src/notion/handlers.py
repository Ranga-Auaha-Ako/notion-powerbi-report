import logging
import backoff
from aiolimiter import AsyncLimiter
from os import getenv
from dotenv import load_dotenv
load_dotenv()  # take environment variables from .env.

from notion_client import AsyncClient
from notion_client import APIErrorCode, APIResponseError, errors



# Obtain the `token_v2` value by inspecting your browser cookies on a logged-in (non-guest) session on Notion.so
notion = AsyncClient(auth=getenv("NOTION_TOKEN"))


# Create rate limiter to prevent hitting Notion API rate limit
# Three requests per second: https://developers.notion.com/reference/limits
# 2700 requests per 15 minutes: https://www.reddit.com/r/Notion/comments/xfufed/how_do_you_handle_request_limits_using_notion_api/j4tlq2n/
limiter = AsyncLimiter(2*60, 1*60)
# limiter = AsyncLimiter(2500, 15*60)

logging.getLogger('backoff').addHandler(logging.StreamHandler())


@backoff.on_exception(backoff.expo,
                      APIResponseError,
                      jitter=backoff.full_jitter,
                      max_tries=8)
@backoff.on_exception(backoff.constant, 
                      errors.HTTPResponseError,
                      interval=10,
                      jitter=backoff.full_jitter,
                      max_tries=8)
@backoff.on_exception(backoff.expo, 
                      errors.RequestTimeoutError,
                      jitter=backoff.full_jitter,
                      max_tries=15) 
async def updateNotionPageBackoff(page_id, properties):
    async with limiter:
        return await notion.pages.update(page_id=page_id, properties=properties)

@backoff.on_exception(backoff.expo, 
                      APIResponseError,
                      jitter=backoff.full_jitter,
                      max_tries=8)
@backoff.on_exception(backoff.constant, 
                      errors.HTTPResponseError,
                      interval=10,
                      jitter=backoff.full_jitter,
                      max_tries=8)
@backoff.on_exception(backoff.expo, 
                      errors.RequestTimeoutError,
                      jitter=backoff.full_jitter,
                      max_tries=15) 
async def createNotionPageBackoff(database_id, properties):
    async with limiter:
        return await notion.pages.create(parent={"database_id": database_id}, properties=properties)

@backoff.on_exception(backoff.expo, 
                      APIResponseError,
                      jitter=backoff.full_jitter,
                      max_tries=8)
@backoff.on_exception(backoff.constant, 
                      errors.HTTPResponseError,
                      interval=10,
                      jitter=backoff.full_jitter,
                      max_tries=8)
@backoff.on_exception(backoff.expo, 
                      errors.RequestTimeoutError,
                      jitter=backoff.full_jitter,
                      max_tries=15) 
async def getNotionPagesBackoff(database_id: str, pbar=None, **kwargs):
    async with limiter:
        # print("Getting notion pages...{}".format(str(kwargs)))
        res = await notion.databases.query(database_id, **kwargs)
        if pbar:
            pbar.update(5)
        return res

@backoff.on_exception(backoff.expo, 
                      APIResponseError,
                      jitter=backoff.full_jitter,
                      max_tries=8)
@backoff.on_exception(backoff.constant, 
                      errors.HTTPResponseError,
                      interval=10,
                      jitter=backoff.full_jitter,
                      max_tries=8) 
@backoff.on_exception(backoff.expo, 
                      errors.RequestTimeoutError,
                      jitter=backoff.full_jitter,
                      max_tries=15) 
async def deleteNotionPageBackoff(page_id, pbar=None):
    async with limiter:
        # print("Deleting notion page...")
        res = await notion.pages.update(page_id=page_id, archived=True)
        if pbar:
            pbar.update(1)
        return res