import multiprocessing as mp
import os
import sqlite3
import time

import requests
from loguru import logger

from examples.pokemon import Pokemon
from examples.database import  clean_db


class ETLWorker(object):
    URI = "./sqlite.db"

    def __init__(self):
        self.token: str = ""
        self.data: dict = {}

    def main(self):
        clean_db(self.URI)
        tokens = self.prepare()
        with mp.Pool(24) as pool:
            for i in range(1, 152):
                pool.apply_async(self.wrapper, args=(i,), kwds={"token_queue": tokens})
            pool.close()
            pool.join()

    def wrapper(self, i: int, token_queue: mp.Queue):
        logger.info(f"Process Pokemon {i}")
        data = self.load(i, token_queue)
        pokemon = self.transform(data)
        self.export(pokemon)

    def prepare(self) -> mp.Queue:
        token = os.environ.get("PSEUDO_API_TOKEN", "pseudo_token")
        manager = mp.Manager()
        token_queue = manager.Queue()
        for _ in range(10):
            token_queue.put(token)
        logger.info("Read token from the environment")
        return token_queue

    def load(self, id: int, token_queue: mp.Queue):
        logger.debug("Get token")
        token = token_queue.get()
        logger.info(f"Load data with pseudo_token {token}")
        try:
            response = requests.get(f"https://pokeapi.co/api/v2/pokemon/{id}")
        finally:
            token_queue.put(token)
        logger.info(f"Received answer with code {response.status_code}")
        logger.debug("Put token")
        return response.json()

    def transform(self, data) -> Pokemon:
        logger.debug("Start Transformation")
        time.sleep(10)  # make this function a little more expansive
        id = data["id"]
        name = data["name"]
        base_stat = data["stats"][0]["base_stat"]
        return Pokemon(id=id, name=name, base_stat=base_stat)

    def export(self, pokemon: Pokemon):
        with sqlite3.connect(self.URI) as con:
            con.execute("Insert into pokemon (id, name, base_stat) Values(?,?,?)",
                        [pokemon.id, pokemon.name, pokemon.base_stat]
                        )
        logger.info("Inserted the pokemon")


if __name__ == '__main__':
    logger.add("download.log")
    etl = ETLWorker()
    etl.main()
    logger.success("Done")
