import multiprocessing as mp
import os
import sqlite3
import time
from typing import Tuple

import requests
from loguru import logger

from .pokemon import Pokemon
from .database import  clean_db



class DbManager():
    URI = "./sqlite.db"

    def __init__(self, pokemon: mp.Queue):

        self.pokemon_queue = pokemon

    def main(self):
        logger.info("DBManger | entering main")
        clean_db(self.URI)
        logger.info("DBManager | cleaned DB")
        continue_loop = True
        with sqlite3.connect(self.URI) as con:
            logger.info("DBManager | Entering Eventloop")
            while continue_loop:
                continue_loop = self.event_loop(con)
            logger.info("DBManager | Terminate")
        logger.info("DBManager | Closed connection")

    def event_loop(self, con: sqlite3.Connection) -> bool:

        pokemon = self.pokemon_queue.get()
        if type(pokemon) == Pokemon:
            logger.debug(f"DBManager | Try to insert {pokemon.name}")
            self.insert_pokemon(con, pokemon)
            logger.debug(f"DBManager | continue eventloop")
            return True
        else:
            logger.debug("DBManager | Received Termination Signal - Exit Eventloop")
            return False

    def insert_pokemon(self, con, pokemon):
        con.execute("Insert into pokemon (id, name, base_stat) Values(?,?,?)",
                    [pokemon.id, pokemon.name, pokemon.base_stat]
                    )
        logger.debug("DBManager | Inserted the pokemon")


class ETLWorker(object):
    CONCURRENCY = 24
    CONCURREND_DOWNLOADS = 10

    def __init__(self):
        self.token: str = ""
        self.data: dict = {}

    def main(self):
        logger.info("ETLWorker | Entering main ")
        token_queue, insert_queue = self.prepare()
        db_manager = DbManager(insert_queue)
        process = mp.Process(target=db_manager.main)
        process.start()
        logger.info("ETLWorker | started DBManager ")
        with mp.Pool(self.CONCURRENCY) as pool:
            logger.info("ETLWorker |Opened pool")
            for i in range(1, 152):
                pool.apply_async(self.wrapper,
                                 args=(i,),
                                 kwds={"token_queue": token_queue, "insert_queue": insert_queue})
            pool.close()
            pool.join()
            logger.info("Send shutdown signal to DBManager")
            insert_queue.put("Terminate")
            process.join()
            logger.info("ETLWorker | Closed Pool")

    def wrapper(self, i: int, token_queue: mp.Queue, insert_queue: mp.Queue):
        logger.debug(f"ETLWorker | Try to load pokemon with id {i}")
        self.load(i, token_queue)
        logger.debug(f"ETLWorker | Loaded Pokemon with id {i}")
        pokemon = self.transform()
        logger.debug(f"ETLWorker | Transform Pokemon with id {i}")
        insert_queue.put(pokemon)
        logger.debug(f"ETLWorker | Handed Pokemon {i} to DBManager")

    def prepare(self) -> Tuple[mp.Queue, mp.Queue]:
        token = os.environ.get("PSEUDO_API_TOKEN", "pseudo_token")
        manager = mp.Manager()
        token_queue = manager.Queue()
        insert_queue = manager.Queue()
        for _ in range(self.CONCURREND_DOWNLOADS):
            token_queue.put(token)
        logger.debug("ETLWorker | read Token and prepared Queues")
        return token_queue, insert_queue

    def load(self, id: int, token_queue: mp.Queue):
        logger.debug("Get token")
        token = token_queue.get()
        logger.info(f"Load data with pseudo_token {self.token}")
        response = requests.get(f"https://pokeapi.co/api/v2/pokemon/{id}")
        logger.info(f"Received answer with code {response.status_code}")
        token_queue.put(token)
        logger.debug("Put token")
        self.data = response.json()

    def transform(self) -> Pokemon:
        logger.debug("Start Transformation")
        time.sleep(10)  # make this function a little more expansive
        id = self.data["id"]
        name = self.data["name"]
        base_stat = self.data["stats"][0]["base_stat"]
        return Pokemon(id=id, name=name, base_stat=base_stat)


if __name__ == '__main__':
    logger.level("DEBUG")
    logger.add("download.log")
    etl = ETLWorker()
    etl.main()
    logger.success("Done")
