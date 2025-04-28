

class JobQueue():
    def __init__(self, client, func):
        self.client = client
        self.list = []
        self.results = []
        self.errors = []
        self.__func__ = func

    def append(self, *args, **kwargs):
        self.list.append((args, kwargs))

    def run(self):
        for i, (args, kwargs) in enumerate(self.list):
            self.list[i] = {
                'args': args,
                'job': self.__func__(*args, **kwargs)
            }

        while len(self.list) > 0:
            for i, j in enumerate(self.list):
                if j['job']:
                    job = self.client.get_job(j['job'].job_id, location=j['job'].location)
                    if job.done():
                        d = self.list.pop(i)
                        try:
                            d['result'] = job.result()
                            self.results.append(d)
                        except Exception as e:
                            d['error'] = str(e)
                            self.errors.append(d)
                else:
                    d = self.list.pop(i)
                    self.results.append(d)

        if len(self.errors) > 0:
            raise Exception('Errors occurred')

        return True
