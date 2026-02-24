import asyncio
import datetime
from asyncio import all_tasks

import aiohttp
from itertools import batched

from db import init_orm, close_orm, SwapiPeople, DbSession

MAX_REQUEST = 5
async def get_people(person_id: int, session: aiohttp.ClientSession):
    http_response = await session.get(f"https://www.swapi.tech/api/people/{person_id}/")
    json_data = await http_response.json()

    clean_json = {}
    list_of_keys = ["_id", "birth_year", "eye_color", "gender", "hair_color", "homeworld", "mass", "name", "skin_color"]

    for key in list_of_keys:
        if key in json_data:
            clean_json[key] = json_data[key]

    return clean_json

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
        for id_batch in batched(MAX_REQUEST):
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
