-- ============================================================
-- CDC Sync Demo: DDL for test.cdc_test and all target tables
-- Execute source table in `test` database, target tables in `wangzhijun` database
-- ============================================================

-- ============================================================
-- Source table (test database)
-- 真实源表结构: id, name, email, age, status, score, created_at, updated_at
-- ============================================================
CREATE DATABASE IF NOT EXISTS `test`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE `test`;

DROP TABLE IF EXISTS `cdc_test`;
CREATE TABLE `cdc_test` (
    `id`         INT(11)      NOT NULL AUTO_INCREMENT,
    `name`       VARCHAR(64)  NOT NULL,
    `email`      VARCHAR(128) DEFAULT NULL,
    `age`        INT(11)      DEFAULT 0,
    `status`     TINYINT(4)   DEFAULT 1 COMMENT '1:active 0:inactive',
    `score`      DECIMAL(10,2)DEFAULT 0.00,
    `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='CDC同步测试源表';

-- 插入测试数据
INSERT INTO `cdc_test` (`name`, `email`, `age`, `status`, `score`, `created_at`, `updated_at`) VALUES
('张三', 'zhangsan@example.com',   25, 1, 85.50, NOW(), NOW()),
('李四', 'lisi@example.com',       30, 0, 92.00, NOW(), NOW()),
('王五', 'wangwu@example.com',     28, 1, 78.25, NOW(), NOW()),
('',     'empty@example.com',      22, 1, 60.00, NOW(), NOW()),  -- 会被 filter_empty_name 过滤
(NULL,   'nullname@example.com',   35, 1, 55.00, NOW(), NOW());  -- 会被 filter_empty_name 过滤


-- ============================================================
-- Target database
-- ============================================================
CREATE DATABASE IF NOT EXISTS `wangzhijun`
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE `wangzhijun`;

-- ============================================================
-- Pattern 1: Single-table target (全量字段直通)
-- test.cdc_test → wangzhijun.cdc_test
-- 字段完全一致, 由 CdcTestMigrationPlugin 处理
-- ============================================================
DROP TABLE IF EXISTS `cdc_test`;
CREATE TABLE `cdc_test` (
    `id`         INT(11)      NOT NULL AUTO_INCREMENT,
    `name`       VARCHAR(64)  NOT NULL,
    `email`      VARCHAR(128) DEFAULT NULL,
    `age`        INT(11)      DEFAULT 0,
    `status`     TINYINT(4)   DEFAULT 1 COMMENT '1:active 0:inactive',
    `score`      DECIMAL(10,2)DEFAULT 0.00,
    `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='CDC同步单表目标表';


-- ============================================================
-- Pattern 2: Shard targets (全量字段, hash(id) % 2 分片)
-- test.cdc_test → wangzhijun.cdc_test_0, cdc_test_1
-- 每个分片表结构与源表完全一致
-- ============================================================
DROP TABLE IF EXISTS `cdc_test_0`;
CREATE TABLE `cdc_test_0` (
    `id`         INT(11)      NOT NULL,
    `name`       VARCHAR(64)  NOT NULL,
    `email`      VARCHAR(128) DEFAULT NULL,
    `age`        INT(11)      DEFAULT 0,
    `status`     TINYINT(4)   DEFAULT 1 COMMENT '1:active 0:inactive',
    `score`      DECIMAL(10,2)DEFAULT 0.00,
    `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='CDC同步分表0';

DROP TABLE IF EXISTS `cdc_test_1`;
CREATE TABLE `cdc_test_1` (
    `id`         INT(11)      NOT NULL,
    `name`       VARCHAR(64)  NOT NULL,
    `email`      VARCHAR(128) DEFAULT NULL,
    `age`        INT(11)      DEFAULT 0,
    `status`     TINYINT(4)   DEFAULT 1 COMMENT '1:active 0:inactive',
    `score`      DECIMAL(10,2)DEFAULT 0.00,
    `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='CDC同步分表1';


-- ============================================================
-- Pattern 3: Narrow split targets (字段子集 + 字段重命名)
-- test.cdc_test → wangzhijun.cdc_test_split_basic, split_info, split_detail
--
-- split_basic:  id, name, contact_email(email→contact_email重命名), status
-- split_info:   id, age, score, created_at
-- split_detail: id, email, created_at, updated_at
-- ============================================================

-- 基础信息表: id, name, contact_email, status
-- 注意: email 重命名为 contact_email (演示 rename 能力)
DROP TABLE IF EXISTS `cdc_test_split_basic`;
CREATE TABLE `cdc_test_split_basic` (
    `id`            INT(11)      NOT NULL COMMENT '主键',
    `name`          VARCHAR(64)  NOT NULL COMMENT '名称',
    `contact_email` VARCHAR(128) DEFAULT NULL COMMENT '联系邮箱 (由源表email重命名)',
    `status`        TINYINT(4)   DEFAULT 1 COMMENT '1:active 0:inactive',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='CDC拆分表-基础信息';

-- 数值信息表: id, age, score, created_at
DROP TABLE IF EXISTS `cdc_test_split_info`;
CREATE TABLE `cdc_test_split_info` (
    `id`         INT(11)      NOT NULL COMMENT '主键',
    `age`        INT(11)      DEFAULT 0 COMMENT '年龄',
    `score`      DECIMAL(10,2)DEFAULT 0.00 COMMENT '分数',
    `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='CDC拆分表-数值信息';

-- 扩展信息表: id, email, created_at, updated_at
DROP TABLE IF EXISTS `cdc_test_split_detail`;
CREATE TABLE `cdc_test_split_detail` (
    `id`         INT(11)      NOT NULL COMMENT '主键',
    `email`      VARCHAR(128) DEFAULT NULL COMMENT '邮箱',
    `created_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    `updated_at` TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='CDC拆分表-扩展信息';
