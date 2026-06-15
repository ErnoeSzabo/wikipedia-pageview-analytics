
-- ============================================================
-- Global Attention Analytics: Wikipedia Pageview Trends
-- Databricks SQL dashboard queries
-- ============================================================


-- ------------------------------------------------------------
-- 01. Total views per topic
-- Ranks all tracked topics by total pageviews.
-- ------------------------------------------------------------

WITH ranked AS (
    SELECT
        topic,
        total_views,
        distinct_matched_articles,
        mobile_views,
        desktop_views,
        mobile_share,
        desktop_share,
        ROW_NUMBER() OVER (
            ORDER BY total_views DESC
        ) AS sort_order
    FROM workspace.wiki_gold.topic_summary
)

SELECT
    topic,
    total_views,
    distinct_matched_articles,
    mobile_views,
    desktop_views,
    mobile_share,
    desktop_share,
    sort_order
FROM ranked
ORDER BY sort_order;


-- ------------------------------------------------------------
-- 02. Mobile versus desktop views by topic
-- ------------------------------------------------------------

SELECT
    topic,
    mobile_views,
    desktop_views
FROM workspace.wiki_gold.topic_summary
ORDER BY total_views DESC;


-- ------------------------------------------------------------
-- 03. Top pages by topic
-- Requires a Databricks SQL parameter named: topic
-- ------------------------------------------------------------

SELECT
    page_rank,
    page_title_clean,
    total_views,
    mobile_views,
    desktop_views
FROM workspace.wiki_gold.top_pages_by_topic
WHERE topic = :topic
  AND page_rank <= 10
ORDER BY page_rank;


-- ------------------------------------------------------------
-- 04. Top pages for selected topic
-- Uses Artificial Intelligence when no topic is selected.
-- ------------------------------------------------------------

SELECT
    page_rank,
    page_title_clean,
    total_views,
    mobile_views,
    desktop_views
FROM workspace.wiki_gold.top_pages_by_topic
WHERE topic = COALESCE(
    NULLIF(:topic, ''),
    'Artificial Intelligence'
)
  AND page_rank <= 10
ORDER BY page_rank;


-- ------------------------------------------------------------
-- 05. Topic views by hour
-- Uses the full UTC timestamp so the rolling window remains
-- chronologically correct when it crosses midnight.
-- ------------------------------------------------------------

SELECT
    view_timestamp,
    topic,
    SUM(total_views) AS total_views
FROM workspace.wiki_gold.topic_pageviews
GROUP BY
    view_timestamp,
    topic
ORDER BY
    view_timestamp,
    topic;


-- ------------------------------------------------------------
-- 06. Current data window
-- Displays the first and last timestamps in the rolling window.
-- ------------------------------------------------------------

SELECT
    COUNT(DISTINCT view_timestamp) AS hourly_slots,
    DATE_FORMAT(
        MIN(view_timestamp),
        'MMM d, HH:mm'
    ) AS window_start_utc,
    DATE_FORMAT(
        MAX(view_timestamp),
        'MMM d, HH:mm'
    ) AS latest_data_utc
FROM workspace.wiki_gold.topic_pageviews;

