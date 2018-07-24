select 'foo' as '';
select r.id as upstream_id, r.foo_token_id as upstream_foo_token_id, r.name as upstream_foo_token_name,
       t.id as upstream_id, t.token as upstream_token
         from things_upstream.foo_ref as r
         join things_upstream.foo_tokens as t
             on r.foo_token_id = t.id;

select r.id as downstream_id, r.foo_token_id as downstream_foo_token_id, r.name as downstream_foo_token_name,
            t.id as downstream_id, t.token  as downstream_token
         from things_downstream.foo_ref as r
         join things_downstream.foo_tokens as t
             on r.foo_token_id = t.id;

select 'bar' as '';
select u.id as upstream_id, u.val as upstream_val, d.id as downstream_id, d.val  as downstream_val
from things_upstream.bar as u
left join things_downstream.bar as d
on u.id = d.id
union
select u.id as upstream_id, u.val as upstream_val, d.id as downstream_id, d.val  as downstream_val
from things_upstream.bar u
right join things_downstream.bar d on u.id = d.id;

select 'baz' as '';
select u.id as upstream_id, u.val as upstream_val, d.id as downstream_id, d.val  as downstream_val
from things_upstream.baz u
left join things_downstream.baz d on u.id = d.id
union
select u.id as upstream_id, u.val as upstream_val, d.id as downstream_id, d.val  as downstream_val
from things_upstream.baz u
right join things_downstream.baz d on u.id = d.id;
