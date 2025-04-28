DROP TABLE IF EXISTS `ml-db`.`site_visitors`;

CREATE TABLE `ml-db`.`site_visitors` AS
SELECT
  s.id AS site_id,
  u.id AS user_id,
  u.tree_user_id,
  atc.third_party_tracking_company,
  atc.tracking_code,
  sv.visitor_id,
  sv.last_visit_date,
  sv.visit_count,
  sv.ipaddress,
  sv.browser_agent,
  sv.created_at
FROM sites s
LEFT OUTER JOIN site_visitors sv ON s.id = sv.site_id
INNER JOIN users u ON s.user_id = u.id
LEFT OUTER JOIN psites_analytics_tracking_codes atc ON u.id = atc.user_id