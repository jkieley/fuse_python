import unittest
import md5


class TestPassthrough(unittest.TestCase):

    def setUp(self):
        self.md5 = md5.Md5Helper()

    def test_1kb(self):
        self.md5.md5_entire_file('./test_files/1K.txt')
        self.assertTrue(True)

    def test_1Mb(self):
        self.md5.md5_entire_file('./test_files/1M.txt')
        self.assertTrue(True)

    def test_10Mb(self):
        self.md5.md5_entire_file('./test_files/10M.txt')
        self.assertTrue(True)

    def test_100Mb(self):
        self.md5.md5_entire_file('./test_files/100M.txt')
        self.assertTrue(True)

    def test_1Gb(self):
        self.md5.md5_entire_file('./test_files/1Gb.txt')
        self.assertTrue(True)

    def test_5Gb(self):
        self.md5.md5_entire_file('./test_files/5Gb.txt')
        self.assertTrue(True)

    def test_10Gb(self):
        self.md5.md5_entire_file('./test_files/10Gb.txt')
        self.assertTrue(True)

if __name__ == '__main__':
    unittest.main()