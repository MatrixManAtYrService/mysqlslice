select * from things_upstream.foo_ref u left join things_downstream.foo_ref d on u.id = d.id;
select * from things_upstream.foo_tokens u left join things_downstream.foo_tokens d on u.id = d.id;
select * from things_upstream.bar u left join things_downstream.bar d on u.id = d.id;
select * from things_upstream.baz u left join things_downstream.baz d on u.id = d.id;
