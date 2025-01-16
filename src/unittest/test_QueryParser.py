import unittest
import sys
from dotenv import load_dotenv
import os

sys.path.append("../")
from beaker import benchmark

load_dotenv("../examples/.env")

hostname = os.getenv("DATABRICKS_HOST")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
# Don't put tokens in plaintext in code
access_token = os.getenv("DATABRICKS_ACCESS_TOKEN")
catalog_name = os.getenv("CATALOG")
schema_name = os.getenv("SCHEMA")

class TestQueryParser(unittest.TestCase):
    def setUp(self):
        self.bm = benchmark.Benchmark()
        # self.bm.setName(name="unittest")
        # self.bm.setHostname(hostname=hostname)
        # self.bm.setWarehouseToken(token=access_token)
        # self.bm.setWarehouse(http_path=http_path)

    def test_get_queries_from_file(self):
        # Define a test case
        test_file_path = '../../examples/queries/q2.sql'
        # replace with the expected output
        expected_output = [("--q02--\nselect 'q2', now();", 'q02', None), ("--q03--\n--query with comment\nselect 'q3', now();", 'q03', None), ("--{q04: 3s}--\n--test with random query identifier\nselect 'q4', now();", '{q04: 3s}', None)]

        # Call the function with the test case
        actual_output = self.bm._get_queries_from_file(test_file_path)

        print(actual_output)

        # Assert that the actual output matches the expected output
        self.assertEqual(actual_output, expected_output)

    
    def test_get_queries_from_dir(self):
        # Define a test case
        test_dir_path = '../../examples/queries/'
        # replace with the expected output
        expected_output = [("--q01--\nselect 'q1', now();", 'q01', None), ("--q02--\nselect 'q2', now();", 'q02', None), ("--q03--\n--query with comment\nselect 'q3', now();", 'q03', None), ("--{q04: 3s}--\n--test with random query identifier\nselect 'q4', now();", '{q04: 3s}', None), ("--q10--\nselect 'q10', now();", 'q10', None)]

        # Call the function with the test case
        self.bm.query_file_format = "semicolon-delimited"
        actual_output = self.bm._get_queries_from_dir(test_dir_path)
        
        # Assert that the actual output matches the expected output
        self.assertEqual(actual_output, expected_output)

    def test_get_queries_from_file_with_param(self):
        # Define a test case
        test_file_path = '../../examples/queries_params/q2.sql'
        test_param_path = '../../examples/queries_params/params.json'
        # replace with the expected output
        expected_output = [("--Q02|{'p_size': 15, 'p_type': '%BRASS', 'r_name': 'EUROPE'}--\nselect\n  s_acctbal,\n  s_name,\n  n_name,\n  p_partkey,\n  p_mfgr,\n  s_address,\n  s_phone,\n  s_comment\nfrom\n  part,\n  supplier,\n  partsupp,\n  nation,\n  region\nwhere\n  p_partkey = ps_partkey\n  and s_suppkey = ps_suppkey\n  and p_size = :p_size\n  and p_type like :p_type\n  and s_nationkey = n_nationkey\n  and n_regionkey = r_regionkey\n  and r_name = :r_name\n  and ps_supplycost = (\n    select\n      min(ps_supplycost)\n    from\n      partsupp,\n      supplier,\n      nation,\n      region\n    where\n      p_partkey = ps_partkey\n      and s_suppkey = ps_suppkey\n      and s_nationkey = n_nationkey\n      and n_regionkey = r_regionkey\n      and r_name = :r_name\n  )\norder by\n  s_acctbal desc,\n  n_name,\n  s_name,\n  p_partkey;", 'Q02', {'p_size': 15, 'p_type': '%BRASS', 'r_name': 'EUROPE'})]

        # Call the function with the test case
        actual_output = self.bm._get_queries_from_file(test_file_path, test_param_path)
        # print(actual_output)

        # Assert that the actual output matches the expected output
        self.assertEqual(actual_output, expected_output)

    def test_get_queries_from_dir_with_param(self):
        # Define a test case
        test_dir_path = '../../examples/queries_params/'
        test_param_path = '../../examples/queries_params/params.json'
        # replace with the expected output
        expected_output = [("--Q01|{'l_shipdate': '1998-12-01'}--\nSELECT\n  l_returnflag,\n  l_linestatus,\n  sum(l_quantity) as sum_qty,\n  sum(l_extendedprice) as sum_base_price,\n  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n  avg(l_quantity) as avg_qty,\n  avg(l_extendedprice) as avg_price,\n  avg(l_discount) as avg_disc,\n  count(*) as count_order\nFROM\n  lineitem\nWHERE\n  l_shipdate <= cast(:l_shipdate as date) - interval '90' day\nGROUP BY\n  l_returnflag,\n  l_linestatus\nORDER BY\n  l_returnflag,\n  l_linestatus;", 'Q01', {'l_shipdate': '1998-12-01'}), ("--Q01|{'l_shipdate': '1998-11-01'}--\nSELECT\n  l_returnflag,\n  l_linestatus,\n  sum(l_quantity) as sum_qty,\n  sum(l_extendedprice) as sum_base_price,\n  sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\n  sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\n  avg(l_quantity) as avg_qty,\n  avg(l_extendedprice) as avg_price,\n  avg(l_discount) as avg_disc,\n  count(*) as count_order\nFROM\n  lineitem\nWHERE\n  l_shipdate <= cast(:l_shipdate as date) - interval '90' day\nGROUP BY\n  l_returnflag,\n  l_linestatus\nORDER BY\n  l_returnflag,\n  l_linestatus;", 'Q01', {'l_shipdate': '1998-11-01'}), ("--Q02|{'p_size': 15, 'p_type': '%BRASS', 'r_name': 'EUROPE'}--\nselect\n  s_acctbal,\n  s_name,\n  n_name,\n  p_partkey,\n  p_mfgr,\n  s_address,\n  s_phone,\n  s_comment\nfrom\n  part,\n  supplier,\n  partsupp,\n  nation,\n  region\nwhere\n  p_partkey = ps_partkey\n  and s_suppkey = ps_suppkey\n  and p_size = :p_size\n  and p_type like :p_type\n  and s_nationkey = n_nationkey\n  and n_regionkey = r_regionkey\n  and r_name = :r_name\n  and ps_supplycost = (\n    select\n      min(ps_supplycost)\n    from\n      partsupp,\n      supplier,\n      nation,\n      region\n    where\n      p_partkey = ps_partkey\n      and s_suppkey = ps_suppkey\n      and s_nationkey = n_nationkey\n      and n_regionkey = r_regionkey\n      and r_name = :r_name\n  )\norder by\n  s_acctbal desc,\n  n_name,\n  s_name,\n  p_partkey;", 'Q02', {'p_size': 15, 'p_type': '%BRASS', 'r_name': 'EUROPE'})]

        # Call the function with the test case
        actual_output = self.bm._get_queries_from_dir(test_dir_path, test_param_path)
        # print(actual_output)

        # Assert that the actual output matches the expected output
        self.assertEqual(actual_output, expected_output)
        
if __name__ == '__main__':
    unittest.main()