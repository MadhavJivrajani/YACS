class Scheduler:

    def __init__(self, strategy):

        self.schedulers = {
            'LL': self.least_loaded,
            'RR': self.round_robin,
            'R': self.random,
        }

        self.strategy = strategy

    def random(self, worker_config, jobs, tasks_pool, slots_free, data, trigger):
        return jobs, tasks_pool, slots_free, []

    def round_robin(self, worker_config, jobs, tasks_pool, slots_free, data, trigger):
        return jobs, tasks_pool, slots_free, []

    def least_loaded(self, worker_config, jobs, tasks_pool, slots_free, data, trigger):
        return jobs, tasks_pool, slots_free, []

    def schedule(self, worker_config, jobs, tasks_pool, slots_free, data, trigger):
        return self.scheduler(worker_config, jobs, tasks_pool, slots_free)