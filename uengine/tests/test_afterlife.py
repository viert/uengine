import flask
import unittest

from uengine import afterlife


class TestAfterlife(unittest.TestCase):

    def setUp(self):
        self.app = flask.Flask("afterlife_test_app")
        afterlife.Afterlife(self.app)
        self.client = self.app.test_client()

        @self.app.route("/")
        def root():
            return "Hello world"

        @self.app.route("/error")
        def error():
            flask.abort(500)

    def test_after_response(self):
        @self.app.after_response
        def after():
            after.called = True

        # Regular request
        after.called = False
        self.client.get("/")
        self.assertTrue(after.called)

        # HTTPException
        after.called = False
        self.client.get("/error")
        self.assertTrue(after.called)

        # Non-existent URL
        after.called = False
        self.client.get("/foobar")
        self.assertTrue(after.called)

        # The callback must be called only after the response is closed
        after.called = False
        response = self.client.get("/", buffered=False)
        response.get_data()
        self.assertFalse(after.called)
        response.close()
        self.assertTrue(after.called)

    def test_after_this_response(self):
        def callback():
            callback.called = True

        live_afterlife = True

        @self.app.route("/test")
        def test():
            if live_afterlife:
                afterlife.after_this_response(callback)
            return "Test"

        # Regular request
        callback.called = False
        live_afterlife = True
        self.client.get("/test")
        self.assertTrue(callback.called)

        # Request does not trigger after_this_response
        callback.called = False
        live_afterlife = False
        self.client.get("/test")
        self.assertFalse(callback.called)

        # Request without after_this_response
        callback.called = False
        live_afterlife = True
        self.client.get("/")
        self.assertFalse(callback.called)

        # The callback must be called only after the response is closed
        callback.called = False
        live_afterlife = True
        response = self.client.get("/test", buffered=False)
        response.get_data()
        self.assertFalse(callback.called)
        response.close()
        self.assertTrue(callback.called)

    def test_order(self):
        results = []

        # after_this_response executed first in the order they were registered
        @self.app.route("/test")
        def test():
            afterlife.after_this_response(lambda: results.append(1))
            afterlife.after_this_response(lambda: results.append(2))
            return "Test"

        # Then global after_response callbacks are executed in the order they were registered
        @self.app.after_response
        def after3():
            results.append(3)

        @self.app.after_response
        def after4():
            results.append(4)

        self.client.get("/test")
        self.assertListEqual(results, [1, 2, 3, 4])

    def test_errors(self):

        @self.app.after_response
        def after1():
            after1.called = True

        @self.app.after_response
        def after_fail():
            raise Exception("Oops, an expected exception happened")

        @self.app.after_response
        def after2():
            after2.called = True

        def callback1():
            callback1.called = True

        def callback_fail():
            raise Exception("Ooopsie, an expected exception happened")

        def callback2():
            callback2.called = True

        @self.app.route("/test")
        def test():
            afterlife.after_this_response(callback1)
            afterlife.after_this_response(callback_fail)
            afterlife.after_this_response(callback2)
            return "Test"

        self.client.get("/test")
        self.assertTrue(after1.called)
        self.assertTrue(after2.called)
        self.assertTrue(callback1.called)
        self.assertTrue(callback2.called)

    def test_context(self):
        self.assertFalse(afterlife.has_context())
        with self.assertRaises(RuntimeError):
            _ = afterlife.g.prop

        @self.app.after_response
        def callback():
            callback.prop_exists = "prop" in afterlife.g
            if callback.prop_exists:
                callback.prop_value = afterlife.g.prop

        @self.app.route("/test/<value>")
        def test(value):
            afterlife.g.prop = value
            return "Test"

        @self.app.route("/test2")
        def test2():
            return "Test2"

        self.client.get("/test/value1")
        self.assertTrue(callback.prop_exists)
        self.assertEqual(callback.prop_value, "value1")
        self.client.get("/test/value2")
        self.assertTrue(callback.prop_exists)
        self.assertEqual(callback.prop_value, "value2")

        # Context should reset every request
        self.client.get("/test2")
        self.assertFalse(callback.prop_exists)

    def test_no_context(self):
        def callback():
            callback.called = True

        afterlife.after_this_response(callback)
        self.assertTrue(callback.called)
