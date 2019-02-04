import unittest
from cpr.Nivel import Nivel
import cpr.information as info
import datetime
import numpy as np

class TestHidraulica(unittest.TestCase):
	def test_algo(self):
		cpr.Nivel(codigo=93,**info.LOCAL)
