create or replace function util.null_if_blank_or_dash(value text)
returns text
language sql
immutable
as $$
    select case
        when value is null then null
        when btrim(value) in ('', '-') then null
        else btrim(value)
    end
$$;

create or replace function util.to_numeric_or_null(value text)
returns numeric
language plpgsql
immutable
as $$
declare
    cleaned text;
begin
    cleaned := replace(util.null_if_blank_or_dash(value), ',', '');
    if cleaned is null then
        return null;
    end if;
    return cleaned::numeric;
exception
    when others then
        return null;
end
$$;

create or replace function util.to_bigint_or_null(value text)
returns bigint
language plpgsql
immutable
as $$
declare
    cleaned text;
begin
    cleaned := replace(util.null_if_blank_or_dash(value), ',', '');
    if cleaned is null then
        return null;
    end if;
    return cleaned::bigint;
exception
    when others then
        return null;
end
$$;

create or replace function util.to_date_compact_or_null(value text)
returns date
language plpgsql
immutable
as $$
declare
    cleaned text;
begin
    cleaned := util.null_if_blank_or_dash(value);
    if cleaned is null then
        return null;
    end if;
    if cleaned ~ '^\d{8}$' then
        return to_date(cleaned, 'YYYYMMDD');
    end if;
    if cleaned ~ '^\d{6}$' then
        return to_date(cleaned || '01', 'YYYYMMDD');
    end if;
    return null;
exception
    when others then
        return null;
end
$$;
