import asyncio
import logging
import unittest

from serverish.messenger import Messenger

logger = logging.getLogger(__name__.rsplit('.')[-1])


class TestPlanManager(unittest.IsolatedAsyncioTestCase):

    def setUp(self):
        super().setUp()

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.messenger = Messenger()
        await self.messenger.open()

    def tearDown(self) -> None:
        super().tearDown()

    async def asyncTearDown(self):
        await self.messenger.close()
        await super().asyncTearDown()

    async def test_1(self):

        await asyncio.sleep(1)


if __name__ == '__main__':
    unittest.main()
