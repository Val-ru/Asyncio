import asyncio
import datetime
import itertools
from asyncio import all_tasks
from http.client import responses
from itertools import batched
from tkinter.font import names

import aiohttp

from config import LIST_OF_KEYS, HAVE_URLS_LIST, MAX_REQUEST
from db import init_orm, close_orm, SwapiPeople, DbSession


async def get_people(person_id: int, session: aiohttp.ClientSession):
    async with session.get(f"https://www.swapi.tech/api/people/{person_id}/") as response:
        if response.status != 200:
            return None
        data = await response.json()
        result = data.get('result', {})
        properties = result.get('properties', {})

        clean_json = {}

        for key in LIST_OF_KEYS:
            # print(key)
            if key in properties:
                if key == 'homeworld':
                    clean_json[key] = await get_planet_name(properties[key], session)
                elif key in HAVE_URLS_LIST:
                    clean_json[key] = await get_names_from_urls(properties[key], session)
                else:
                    clean_json[key] = properties[key]
        clean_json['id'] = person_id
        return clean_json


async def get_names_from_urls(urls: list, session: aiohttp.ClientSession):

    if not urls:
        return ""
    coros = [session.get(url) for url in urls]
    responses = await asyncio.gather(*coros)

    names = []
    for response in responses:
        if response.status == 200:
            data = await response.json()

            if 'result' in data:
                properties = data['result'].get('properties', {})
                names.append(properties.get('name') or properties.get('title', ''))
    return ", ".join(filter(None, names))


async def get_planet_name(url: str, session: aiohttp.ClientSession):

    if not url:
        return ""

    coro = session.get(url)
    response = await coro

    name = ""
    if response.status == 200:
        data = await response.json()
        if 'result' in data:
            properties = data['result'].get('properties', {})
            name += properties.get('name')
    return name


async def get_total_count(session):
    async with session.get("https://www.swapi.tech/api/people/") as response:
        data = await response.json()
        return data.get('count', 0)


async def insert_people_batch(people_list: list[dict]):
    async with DbSession() as db_session:
        # for people in people_list:
        #     people_orm_obj = SwapiPeople(json=people)
        #     db_session.add(people_orm_obj)

        people_orm_obj = [SwapiPeople(json=people) for people in people_list]
        db_session.add_all(people_orm_obj)
        await db_session.commit()


async def main():
    await init_orm()
    async with aiohttp.ClientSession() as http_session:
        for id_batch in batched(get_total_count(http_session), MAX_REQUEST):
            coros = []
            for i in id_batch:
                coro = get_people(i, http_session)
                coros.append(coro)
            # coros = [get_people(i, session) for i in id_batch]
            response = await asyncio.gather(*coros)
            insert_people_batch_coro = insert_people_batch(response)
            insert_people_batch_task = asyncio.create_task(insert_people_batch_coro)
            print(response)

    all_tasks = asyncio.all_tasks()
    main_task = asyncio.current_task()
    all_tasks.remove(main_task)

    for task in all_tasks:
        await task
    await close_orm()

start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)
