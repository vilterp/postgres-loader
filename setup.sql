-- UPLOADS TABLE

drop table if exists uploads;
create table uploads (
  table_name text primary key,
  latest_chunk_idx int,
  finished_upload_at timestamp
);

create or replace function notify_latest_chunk_idx_updated() returns trigger as $$
  declare
  begin
    PERFORM pg_notify('new_chunks', NEW.table_name || ',' || NEW.latest_chunk_idx);
    return new;
  END
$$ language plpgsql;

create trigger notify_on_latest_chunk_idx_updated after update on uploads
for each row execute procedure notify_latest_chunk_idx_updated();

create or replace function notify_done_upload() returns trigger as $$
  declare
  begin
    if NEW.finished_upload_at is not null then
      PERFORM pg_notify('upload_done', NEW.table_name);
    end if;
    return new;
  end;
$$ language plpgsql;

create trigger notify_on_upload_done after update on uploads
for each row execute procedure notify_done_upload();

-- CRIMES TABLE (these would be created dynamically)

drop table if exists crimes;
create table crimes (
  row_idx int primary key,
  chunk_idx int,
  data jsonb
);
create index CONCURRENTLY chunk_idx on crimes (chunk_idx);

-- vv useful for re-runs
update uploads set latest_chunk_idx = null, finished_upload_at = null where table_name = 'crimes';
