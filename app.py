import asyncio
import datetime
import aiohttp
import requests
from more_itertools import chunked
from models import Base, Session, SwapiPeople, engine

MAX_CHUNK_SIZE = 10


def max_people():
    request = requests.get("https://swapi.dev/api/people/")
    return request.json()['count']


async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.dev/api/people/{people_id}")
    if response.status == 404:
        await session.close()
    else:
        json_data = await response.json()
        json_data['id'] = people_id
        await session.close()
        return json_data


async def insert_to_db(people_json_list):
    async with engine.begin() as con:
        await con.run_sync(Base.metadata.create_all)

    async with Session() as session:

        swapi_people_list = [
                    SwapiPeople(
                        id=json_data['id'],
                        birth_year=json_data['birth_year'],
                        eye_color=json_data['eye_color'],
                        films=', '.join(json_data['films']),
                        gender=json_data['gender'],
                        hair_color=json_data['hair_color'],
                        height=json_data['height'],
                        homeworld=json_data['homeworld'],
                        mass=json_data['mass'],
                        name=json_data['name'],
                        skin_color=json_data['skin_color'],
                        species=', '.join(json_data['species']),
                        starships=', '.join(json_data['starships']),
                        vehicles=', '.join(json_data['vehicles']),
                    ) for json_data in people_json_list if json_data is not None
        ]
        session.add_all(swapi_people_list)
        await session.commit()


async def main(characters):
    for ids_chunk in chunked(range(1, characters), MAX_CHUNK_SIZE):
        get_people_coros = [get_people(people_id) for people_id in ids_chunk]
        people_json_list = await asyncio.gather(*get_people_coros)
        asyncio.create_task(insert_to_db(people_json_list))

    current_task = asyncio.current_task()
    tasks_sets = asyncio.all_tasks()
    tasks_sets.remove(current_task)

    await asyncio.gather(*tasks_sets)
    await engine.dispose()


if __name__ == '__main__':
    start = datetime.datetime.now()
    characters = max_people()
    asyncio.run(main(characters))
    print(datetime.datetime.now() - start)
