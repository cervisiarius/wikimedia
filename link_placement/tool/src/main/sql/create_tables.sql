CREATE TABLE `marginal_gains` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `source` varchar(512) NOT NULL,
  `target` varchar(512) NOT NULL,
  `date` datetime DEFAULT NULL,
  `marg_gain_dice` float DEFAULT NULL,
  `marg_gain_coins_link` float DEFAULT NULL,
  `marg_gain_coins_page` float DEFAULT NULL,
  `source_count` int(11) NOT NULL,
  `path_count` int(11) NOT NULL,
  `search_count` int(11) NOT NULL,
  `most_common_path` varchar(512) DEFAULT NULL,
  `most_common_path_count` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_marginal_gains_source` (`source`),
  KEY `idx_marginal_gains_target` (`target`),
  KEY `idx_marginal_gains_date` (`date`),
  KEY `idx_marginal_gains_marg_gain_dice` (`marg_gain_dice`),
  KEY `idx_marginal_gains_marg_gain_coins_link` (`marg_gain_coins_link`),
  KEY `idx_marginal_gains_marg_gain_coins_page` (`marg_gain_coins_page`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;

CREATE TABLE `blacklist` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `marg_gain_id` int(11) NOT NULL,
  `timestamp` datetime NOT NULL,
  `reason` varchar(128) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_blacklist_marg_gain_id` (`marg_gain_id`),
  KEY `idx_blacklist_timestamp` (`timestamp`),
  KEY `idx_blacklist_reason` (`reason`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;

CREATE TABLE `new_link_usage` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `marg_gain_id` int(11) NOT NULL,
  `date` datetime NOT NULL,
  `click_count` int(11) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_new_link_usage_marg_gain_id` (`marg_gain_id`),
  KEY `idx_new_link_usage_date` (`date`),
  KEY `idx_new_link_usage_click_count` (`click_count`)
) ENGINE=InnoDB AUTO_INCREMENT=0 DEFAULT CHARSET=utf8;
