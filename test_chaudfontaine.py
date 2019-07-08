import unittest
from chaudfontaine import Chaudfontaine
import bs4 as bs
fn = 'test/test_datacoverage.pickle'
etl = Chaudfontaine(filename=fn)

class Test_get_stations_db(unittest.TestCase):
    def test_vanilla(self):
        df = etl.get_stations_db('precipitation')
        self.assertGreater(len(df), 1, msg='returned dataframe must be greater than 1 record')

class Test_get_quantity_ids_db(unittest.TestCase):
    def test_vanilla(self):
        d = etl.get_quantity_ids_db()
        k = ['precipitation', 'hauteur', 'debit']
        self.assertEqual(list(d.keys()), k, msg='returned dict should have specific keys')

class Test_build_url_StatHoraireTab(unittest.TestCase):
    def test_vanilla(self):
        url = etl.build_url_StatHoraireTab(
            station_type='precipitation',
            station_code=9596
            )
        self.assertIs(type(url), str, msg='the built url must be a string')
        self.assertEqual(url, 'http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Archive/annuaires/stathorairetab.do?code=95960015&xt=prt')

class Test_retrieveStatHoraireTab(unittest.TestCase):
    def test_vanilla(self):
        url = 'http://voies-hydrauliques.wallonie.be/opencms/opencms/fr/hydro/Archive/annuaires/stathorairetab.do?code=95960015&xt=prt'
        soup = etl.retrieveStatHoraireTab(url)
        self.assertIsInstance(soup, bs.BeautifulSoup)

class Test_parsePeriod(unittest.TestCase):
    def test_vanilla(self):
        sample = open('test/test_samples/stathoraire_95960015.html', mode='r')
        sauce = sample.read()
        sample.close()
        soup = bs.BeautifulSoup(sauce, 'lxml')
        (start_date, end_date) = etl.parsePeriod(soup)
        # 2002/01/01 00:00:00+01 2019/07/01 00:00:00+01
        self.assertIs(type(start_date), str, msg='start_date must be a string')
        self.assertIs(type(end_date), str, msg='end_date must be a string')
        self.assertEqual(start_date, '2002/01/01 00:00:00+01', 'date must have specific value')
        self.assertEqual(end_date, '2019/07/01 00:00:00+01', 'date must have specific value')
