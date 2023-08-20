import unittest
from serverish.base.idmanger import gen_id

class TestIdManager(unittest.TestCase):
    def test_gen_id(self):
        self.assertEqual(gen_id('test'), 'test')
        self.assertEqual(gen_id('test'), 'test-2')
        self.assertEqual(gen_id('test-{:04d}'), 'test-0001')
        self.assertEqual(gen_id('test-{:04d}'), 'test-0002')


if __name__ == '__main__':
    unittest.main()
