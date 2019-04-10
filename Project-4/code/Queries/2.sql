SELECT SUM(c_acctbal), c_name ,c_custkey
FROM customer, orders 
WHERE c_custkey = o_custkey AND o_totalprice < 10000.0
GROUP BY c_name

