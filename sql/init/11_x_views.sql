create or replace view analytics.monitored_x_posts as
select
    (p.created_at at time zone 'Asia/Tokyo')::date as post_date_jst,
    p.created_at at time zone 'Asia/Tokyo' as created_at_jst,
    m.target_username,
    p.author_user_id,
    p.author_username,
    u.name as author_name,
    u.protected as is_protected_source,
    p.post_id,
    ('https://x.com/' || p.author_username || '/status/' || p.post_id) as tweet_url,
    p.conversation_id,
    p.lang,
    p.text,
    util.to_bigint_or_null(p.public_metrics_json ->> 'like_count') as like_count,
    util.to_bigint_or_null(p.public_metrics_json ->> 'reply_count') as reply_count,
    util.to_bigint_or_null(p.public_metrics_json ->> 'quote_count') as quote_count,
    util.to_bigint_or_null(coalesce(
        p.public_metrics_json ->> 'repost_count',
        p.public_metrics_json ->> 'retweet_count'
    )) as repost_count,
    util.to_bigint_or_null(p.public_metrics_json ->> 'bookmark_count') as bookmark_count,
    util.to_bigint_or_null(p.public_metrics_json ->> 'impression_count') as impression_count,
    p.created_at,
    p.first_seen_at,
    p.last_seen_at,
    p.public_metrics_json,
    p.referenced_posts_json,
    p.entities_json,
    p.attachments_json,
    p.payload
from raw.x_posts p
join ingest.x_monitored_accounts m
    on m.target_user_id = p.author_user_id
left join raw.x_users u
    on u.user_id = p.author_user_id
where m.is_active;
