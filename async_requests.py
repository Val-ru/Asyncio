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
    http_response = await session.get(f"https://www.swapi.tech/api/people/{person_id}/")
    json_data = await http_response.json()

    clean_json = {}

    for key in LIST_OF_KEYS:
        # print(key)
        if key in json_data:
            for k, v in json_data.items():
                if k.startswith("homeworld"):
                    http_response = await session.get(f"https://www.swapi.tech/api/planets/\d+/")
                    json_key = await http_response.json()
                    clean_json[key] = json_key["name"]
                if k in HAVE_URLS_LIST:
                    responses = await get_urls_list(person_id, session)
                    clean_json[key] = responses
                clean_json[key] = json_data[key]
    return clean_json


async def get_urls_list(person_id: int, session: aiohttp.ClientSession):
    http_response = await session.get(f"https://www.swapi.tech/api/people/{person_id}/")
    json_data = await http_response.json()

    clean_names = ""

    for url_name in HAVE_URLS_LIST:
        if url_name in json_data:
            coros = []

            for el in json_data[url_name]:
                ### json_data[url] - список ссылок (например, фильмов)
                ### el - фильм из списка
                coros.append(el)
                responses = await asyncio.gather(*coros)
                ### responses - список со словарями

                for dict_ in responses:
                    if url_name == "films":
                        clean_names = ", ".join(dict_["title"])
                    else:
                        clean_names = ", ".join(dict_["name"])
    return clean_names


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
        for id_batch in batched(range(1, 101), MAX_REQUEST):
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
