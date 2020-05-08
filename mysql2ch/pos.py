import abc
import redis


class LogPos:

    @abc.abstractmethod
    def set_log_pos_master(self, master_host, master_port, relay_master_log_file, exec_master_log_pos):
        pass

    @abc.abstractmethod
    def set_log_pos_slave(self, log_file, log_pos):
        pass

    @abc.abstractmethod
    def get_log_pos(self):
        pass


class RedisLogPos(LogPos):
    def __init__(self, host='127.0.0.1', port=6379, password=None, db=0, log_pos_prefix=None, server_id=None):
        self.server_id = server_id
        self.log_pos_prefix = log_pos_prefix
        self.key = f'{log_pos_prefix}:{server_id}'
        pool = redis.ConnectionPool(host=host, port=port, db=db, password=password, decode_responses=True)
        self.redis = redis.StrictRedis(connection_pool=pool)

    def set_log_pos_master(self, master_host, master_port, relay_master_log_file, exec_master_log_pos):
        self.redis.hmset(self.key, {
            'master_host': master_host,
            'master_port': master_port,
            'relay_master_log_file': relay_master_log_file,
            'exec_master_log_pos': exec_master_log_pos
        })

    def set_log_pos_slave(self, log_file, log_pos):
        self.redis.hmset(self.key, {
            'log_pos': log_pos,
            'log_file': log_file
        })

    def get_log_pos(self):
        log_position = self.redis.hgetall(self.key)
        return log_position.get('log_file'), log_position.get('log_pos')
