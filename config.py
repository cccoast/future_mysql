import os


def get_db_connect_str(db_name):
    host = os.getenv("MYSQL_HOST", "127.0.0.1")
    port = os.getenv("MYSQL_PORT", "3306")
    user = os.getenv("MYSQL_USER", "xudi")
    password = os.getenv("MYSQL_PASSWORD", "123456")
    charset = os.getenv("MYSQL_CHARSET", "utf8")
    return "mysql+pymysql://{0}:{1}@{2}:{3}/{4}?charset={5}".format(
        user, password, host, port, db_name, charset)
