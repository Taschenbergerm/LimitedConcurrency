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
        self.token = self.prepare()
        with mp.Pool(10) as pool:
            pool.map(self.wrapper, iterable=range(1,152))

    def wrapper(self, i: int):
        logger.info(f"Process Pokemon {i}")
        data = self.load(i)
        pokemon = self.transform(data)
        self.export(pokemon)

    def prepare(self) -> str:
        token = os.environ.get("PSEUDO_API_TOKEN", "pseudo_token")
        logger.info("Read token from the environment")
        return token

    def load(self, id: int):
        logger.info(f"Load data with pseudo_token {self.token}")
        response = requests.get(f"https://pokeapi.co/api/v2/pokemon/{id}")
        logger.info(f"Received answer with code {response.status_code}")
        return response.json()

    def transform(self, data: dict) -> Pokemon:
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
