from config import cfg
import psycopg2

def connect():
    return psycopg2.connect(host=cfg.db.host, 
                            dbname=cfg.db.dbname, 
                            user=cfg.db.user, 
                            password=cfg.db.password)