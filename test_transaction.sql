begin
insert 1 user1 user1@example.com
select
rollback
select
begin
insert 2 user2 user2@example.com
commit
select
.exit
