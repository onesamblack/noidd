import logging
import argparse
import yaml
import sys
import asyncio
from watchers import Watcher
from notifiers import *
from utils import leveldb_aget


parser = argparse.ArgumentParser()
parser.add_argument("-c","--config", help="configuration file (.yml)", default="/etc/noidd/config.yml")
args = parser.parse_args()

try:
    config = yaml.loads(args.config)

except Exception as e:
    print("couldn't initialize noidd - an exception occurred while loading the config.yml")
    print(str(e))
    sys.exit(1)


noidd_root = "/etc/noidd" if not config["noidd_root"] else config["noidd_root"]
leveldb_file = f"{noid_root}/noidd.db" if not config["leveldb"] else config["leveldb"]
logfile = f"{noid_root}/noidd.log" if not config["logfile"] else config["logfile"]

logging.basicConfig(filename=logfile,
                    filemode='a',
                    format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
                    datefmt='%H:%M:%S',
                    level=logging.DEBUG)

logging.info("Starting noidd")

logger = logging.getLogger('noidd')


class Noidd:
    def __init__(self):
        self.watchers = []
        self.db = None
        self.complete = False

async def db_initialize():
    has_leveldb = await aiofiles.os.exists(leveldb_file)
    if any(not has_leveldb, config["leveldb"]["recreate"]):
        logger.info(f"creating a new db instance at {leveldb_file}")
        create = True
    else:
        logger.info(f"found an existing db")
        create = False

    db = await asyncio.to_thread(plyvel.DB(leveldb_file, create_if_not_exists=create))
    return db
    

async def initialize():
    db = await(db_initialize())
    logger.info("initializing")
    noidd = Noidd()
    notifiers = []
    watchers = [] 
    for n in config["notifiers"]:
        if n["type"] == "twilio":
            n = TwilioNotifier(twilio_api_key=n["twilio_api_key"], twilio_auth_sid_token=n["twilio_auth_sid_token"], from_number=n["twilio_from_number"], recipient_numbers=n["recipients"], batch=n["batch"])
        if n['type'] == "stdout":

    for w in config["watch"]:
        n = w["name"]
        if w["type"] == "dir":
            logger.info(f"creating watch: {name} for {w['root_dir']}")
            pfx_db = db.prefixed_db(f"{name}_")
            init_ts = await leveldb_aget(db = pfxdb, key="initialized")
            if not init_ts:
                initialized = False
            else:
                initialized = True
            watcher = Watcher(name=n, db=pfx_db, initialized=initialized, notifiers=notifiers, root_dir=w["root_dir"])
            watchers.append(watcher)
        if w["type"] == "glob":
            logger.info(f"creating watch: {name} with glob: {w['glob']}")
            pfx_db = db.prefixed_db(f"{name}_")
            init_ts = await leveldb_aget(db = pfxdb, key="initialized")
            if not init_ts:
                initialized = False
            else:
                initialized = True
            watcher = Watcher(name=n, db=pfx_db, initialized=initialized, notifiers=notifiers, root_glob=w["glob"])
            watchers.append(watcher)
        if w["type"] == "filelist":
            logger.info(f"creating watch: {name} with filelist")
            pfx_db = db.prefixed_db(f"{name}_")
            init_ts = await leveldb_aget(db = pfxdb, key="initialized")
            if not init_ts:
                initialized = False
            else:
                initialized = True
            watcher = Watcher(name=n, db=pfx_db, initialized=initialized, notifiers=notifiers, root_filelist=w["filelist"])
            watchers.append(watcher)





def main(noidd):
    pass
    

if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(initialize())
    loop.run_forever(main())
