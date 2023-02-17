import os
import multiprocessing
from multiprocessing import Process
from main import Main
from config import Config
from dotenv import load_dotenv

load_dotenv()

class Parallel(multiprocessing.Process):

    def __init__(self, config) -> None:
        self.config = config
        print(config)

    def run(self) -> None:
        print("Some text")
        # Main().run(self.config)


if __name__ == "__main__":

        configs = Config(os.getenv("CONFIG_FILES_PATH")).get_config()
        for config in configs:
            for table in config["tables"]:
                # p = Parallel(table)
                p = Process(target=Main().run, args=(table, ))
                p.start()
                p.join()
            #     break
            # break


