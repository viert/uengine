from uengine.queue import BaseTask


class ExampleTask(BaseTask):

    @classmethod
    def create(cls, example_id):
        return ExampleTask({"example_id": example_id})

    @property
    def example_id(self):
        return self.data["example_id"]


ExampleTask.register()
