insert 1 user1 user1@example.com
insert 2 user2 user2@example.com
insert into orders 101 1 Apple
insert into orders 102 1 Banana
insert into orders 103 2 Cherry
select join orders on users.id = orders.user_id
.exit
