insert 1 user1 user1@example.com
insert 2 user2 user2@example.com
insert into orders select id, id, 'AutoImport' from users where username = user1
select join orders on users.id = orders.user_id
.exit
