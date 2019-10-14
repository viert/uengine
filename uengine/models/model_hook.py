

class ModelHook(object):
    @classmethod
    def on_hook_register(cls, model_class):
        pass

    @classmethod
    def on_hook_unregister(cls, model_class):
        pass

    @classmethod
    def on_model_init(cls, model):
        pass

    def on_model_destroy(self, model):
        pass

    def on_model_save(self, model, is_new):
        pass
