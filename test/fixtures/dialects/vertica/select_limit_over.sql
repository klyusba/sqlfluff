select a, b from sch.foo
limit 1 over(partition by a order by b)
